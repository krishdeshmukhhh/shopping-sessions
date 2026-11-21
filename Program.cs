using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;

readonly record struct Candidate(ulong Id, long Ingest, int Shard, int Index);
readonly record struct Event(ulong Person, long Time, int Price, int Category);
readonly record struct Purchase(long Time, int Price, int Category);
readonly record struct Click(ulong Person, long Time, ulong Id, int Campaign);
readonly record struct UserDay(ulong Person, int Day, long Sessions, long Events, long Revenue, int Category);
readonly record struct CampaignDay(int Campaign, int Day, long Sessions, long Purchases, long Revenue, long Assists);

sealed class Links(int capacity)
{
    private readonly Dictionary<ulong, int> index = new(capacity);
    private readonly List<int> parent = new(capacity), sizes = new(capacity);
    private readonly List<ulong> minimum = new(capacity);

    private int Add(ulong key)
    {
        ref int entry = ref CollectionsMarshal.GetValueRefOrAddDefault(index, key, out bool exists);
        if (exists) return entry;
        int i = parent.Count;
        entry = i;
        parent.Add(i);
        sizes.Add(1);
        minimum.Add(key);
        return i;
    }

    private int Root(int i)
    {
        while (i != parent[i])
        {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        return i;
    }

    public void Join(ulong a, ulong b, Keys keys)
    {
        int left = Root(Add(a)), right = Root(Add(b));
        if (left == right) return;
        if (sizes[left] < sizes[right]) (left, right) = (right, left);
        parent[right] = left;
        sizes[left] += sizes[right];
        sizes[right] = 0;
        if (keys.Compare(minimum[right], minimum[left]) < 0)
            minimum[left] = minimum[right];
    }

    public (Dictionary<ulong, int> Ids, List<ulong> Names) Finish()
    {
        var names = new List<ulong>();
        var groups = new int[parent.Count];
        for (int i = 0; i < parent.Count; i++)
            if (parent[i] == i)
            {
                groups[i] = names.Count;
                names.Add(minimum[i]);
            }
        foreach (var (key, i) in index) index[key] = groups[Root(i)];
        return (index, names);
    }

}

sealed class Pipeline(string data, string output, int cpus)
{
    private const int ShardCount = 64;
    private readonly ParallelOptions parallel = new() { MaxDegreeOfParallelism = cpus };
    private readonly Keys keys = new();
    private readonly List<ulong> categories = [];
    private readonly List<(ulong Id, ulong Channel)> campaigns = [];
    private Dictionary<ulong, int> products = null!, campaignIds = null!;
    private long start, end;

    private IEnumerable<string> Files(string folder) =>
        Directory.Exists(Path.Combine(data, folder))
            ? Directory.EnumerateFiles(Path.Combine(data, folder), "*.csv")
                .Where(path => path.EndsWith(".csv", StringComparison.Ordinal))
            : [];

    private delegate void Pair(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);

    private void Pairs(IEnumerable<string> files, Pair use)
    {
        Input.ReadFiles(files, new ParallelOptions { MaxDegreeOfParallelism = 1 }, (chunk, _) =>
        {
            Span<Field> fields = stackalloc Field[2];
            while (!chunk.IsEmpty)
            {
                var row = Input.Line(ref chunk);
                if (Input.Fields(row, fields)) use(fields[0].Get(row), fields[1].Get(row));
            }
        });
    }

    private void LoadDimensions()
    {
        using var manifest = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(data, "manifest.json")));
        start = manifest.RootElement.GetProperty("window_start").GetDateTimeOffset().ToUnixTimeMilliseconds();
        end = manifest.RootElement.GetProperty("window_end").GetDateTimeOffset().ToUnixTimeMilliseconds();

        products = [];
        categories.Add(keys.Encode("unknown"u8));
        var categoryIds = new Dictionary<ulong, int> { [categories[0]] = 0 };
        Pairs([Path.Combine(data, "products.csv")], (product, category) =>
        {
            ulong key = keys.Encode(category);
            if (!categoryIds.TryGetValue(key, out int index))
            {
                index = categories.Count;
                categoryIds.Add(key, index);
                categories.Add(key);
            }
            products.Add(keys.Encode(product), index);
        });

        campaignIds = [];
        ulong organic = keys.Encode("organic"u8);
        campaigns.Add((organic, organic));
        Pairs([Path.Combine(data, "campaigns.csv")], (campaign, channel) =>
        {
            ulong id = keys.Encode(campaign);
            campaignIds.Add(id, campaigns.Count);
            campaigns.Add((id, keys.Encode(channel)));
        });
    }

    private readonly record struct Edge(ulong A, ulong B, int BucketA, int BucketB);
    private byte[] routes = [];
    private readonly List<Edge>[] linkParts = Buckets<Edge>();

    // Join hash buckets first, solely to route input. Colliding user IDs share
    // a worker partition. The exact-key join below still decides identity.
    private void RouteLinks()
    {
        var files = Files("links").ToArray();
        long bytes = files.Sum(path => new FileInfo(path).Length);
        int buckets = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Clamp(bytes / 4, 64, 1 << 25));
        int mask = buckets - 1;
        var parents = new int[buckets];
        var edges = new List<Edge>((int)Math.Min(8_000_000, bytes / 22));
        int Root(int bucket)
        {
            while (parents[bucket] > 0)
            {
                int next = parents[bucket] - 1;
                if (parents[next] > 0) parents[bucket] = parents[next];
                bucket = next;
            }
            return bucket;
        }
        Pairs(files, (a, b) =>
        {
            if (a.IsEmpty || b.IsEmpty) return;
            ulong left = keys.Encode(a), right = keys.Encode(b);
            if (left == right) return;
            int x = (int)(Hash(left) & (uint)mask), y = (int)(Hash(right) & (uint)mask);
            edges.Add(new Edge(left, right, x, y));
            int l = Root(x), r = Root(y);
            if (l == r) return;
            if (parents[l] > parents[r]) (l, r) = (r, l);
            parents[l] += parents[r] - 1;
            parents[r] = l + 1;
        });
        if (edges.Count == 0) return;
        routes = new byte[buckets];
        for (int i = 0; i < buckets; i++) routes[i] = (byte)(i & (ShardCount - 1));
        foreach (var edge in edges)
        {
            int part = Root(edge.BucketA) & (ShardCount - 1);
            routes[edge.BucketA] = routes[edge.BucketB] = (byte)part;
            linkParts[part].Add(edge);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int PersonShard(ulong key) =>
        routes.Length == 0 ? Shard(key) : routes[(int)(Hash(key) & (uint)(routes.Length - 1))];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Hash(ulong key)
    {
        key ^= key >> 30;
        key *= 0xbf58476d1ce4e5b9UL;
        key ^= key >> 27;
        key *= 0x94d049bb133111ebUL;
        return key ^ (key >> 31);
    }

    private static int Shard(ulong key) => (int)(Hash(key) & (ShardCount - 1));

    private static List<T>[] Buckets<T>() => Buckets<T>(0);

    private static List<T>[] Buckets<T>(int capacity) =>
        Enumerable.Range(0, ShardCount).Select(_ => new List<T>(capacity)).ToArray();

    private sealed class EventBuffers(int capacity)
    {
        public readonly List<Event>[] People = Buckets<Event>(capacity);
        public readonly List<Candidate>[] Ids = Buckets<Candidate>(capacity);
    }

    private readonly record struct Winner(ulong Id, int Worker, int Index);

    private IList<List<Event>[]> LoadEvents()
    {
        var files = Files("events").ToArray();
        long bytes = files.Sum(path => new FileInfo(path).Length);
        int capacity = (int)Math.Min(262144, bytes / (72L * cpus * ShardCount));
        using var local = new ThreadLocal<EventBuffers>(() => new EventBuffers(capacity), true);
        Input.ReadFiles(files, parallel, (chunk, _) => ParseEvents(chunk, local.Value!));
        var workers = local.Values;

        // Events already reside in person partitions. Only duplicates need
        // a write to those partitions once all parser threads have finished.
        Parallel.For(0, ShardCount, parallel, shard =>
        {
            int count = workers.Sum(worker => worker.Ids[shard].Count);
            if (count == 0) return;
            // Repeated hash positions include every duplicate ID. Resolve their
            // full IDs in a smaller table, allowing singleton positions to pass.
            int bits = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(64, count * 12));
            var seen = new ulong[bits / 64];
            var repeated = new ulong[bits / 64];
            int candidates = 0;
            foreach (var worker in workers)
            {
                foreach (ref readonly var row in CollectionsMarshal.AsSpan(worker.Ids[shard]))
                {
                    int bit = (int)((Hash(row.Id) >> 6) & (uint)(bits - 1));
                    ulong flag = 1UL << bit;
                    ref ulong word = ref seen[bit >> 6];
                    if ((word & flag) != 0)
                    {
                        ref ulong collisions = ref repeated[bit >> 6];
                        if ((collisions & flag) == 0) candidates++;
                        candidates++;
                        collisions |= flag;
                    }
                    word |= flag;
                }
            }
            var slots = new Winner[BitOperations.RoundUpToPowerOf2((uint)(candidates + candidates / 2 + 1))];
            for (int w = 0; w < workers.Count; w++)
            {
                var rows = CollectionsMarshal.AsSpan(workers[w].Ids[shard]);
                for (int i = 0; i < rows.Length; i++)
                {
                    ref readonly var row = ref rows[i];
                    ulong hash = Hash(row.Id) >> 6;
                    int bit = (int)(hash & (uint)(bits - 1));
                    if ((repeated[bit >> 6] & (1UL << bit)) == 0) continue;
                    int slot = (int)((hash ^ (hash >> 19)) & (uint)(slots.Length - 1));
                    while (slots[slot].Id != 0 && slots[slot].Id != row.Id)
                        slot = (slot + 1) & (slots.Length - 1);
                    ref var winner = ref slots[slot];
                    if (winner.Id != 0)
                    {
                        var previousWorker = workers[winner.Worker];
                        var previous = previousWorker.Ids[shard][winner.Index];
                        if (previous.Ingest <= row.Ingest)
                        {
                            Invalidate(workers[w], row);
                            continue;
                        }
                        Invalidate(previousWorker, previous);
                    }
                    winner = new Winner(row.Id, w, i);
                }
            }
            foreach (var worker in workers) worker.Ids[shard] = null!;
        });
        return workers.Select(worker => worker.People).ToArray();
    }

    private static void Invalidate(EventBuffers worker, Candidate row)
    {
        if (row.Index >= 0) CollectionsMarshal.AsSpan(worker.People[row.Shard])[row.Index] = default;
    }

    private void ParseEvents(ReadOnlySpan<byte> chunk, EventBuffers buffers)
    {
        Span<Field> fields = stackalloc Field[7];
        while (!chunk.IsEmpty)
        {
            if (!Input.ReadRow(ref chunk, fields, out var row) || fields[0].Get(row).IsEmpty) continue;
            ulong id = keys.Encode(fields[0].Get(row));
            long ingest = Input.IngestTimestamp(fields[3].Get(row));
            long time = Input.Timestamp(fields[2].Get(row));
            ulong person = 0;
            int price = 0, category = 0;

            if (time >= start && time < end && ingest - time <= Input.Day &&
                !fields[1].Get(row).IsEmpty && !fields[5].Get(row).IsEmpty)
            {
                var type = fields[4].Get(row);
                if (type.SequenceEqual("view"u8) || type.SequenceEqual("cart"u8))
                    person = keys.Encode(fields[1].Get(row));
                else if (type.SequenceEqual("purchase"u8))
                {
                    long amount = Input.Number(fields[6].Get(row));
                    if (amount is >= 1 and <= 10_000_000)
                    {
                        person = keys.Encode(fields[1].Get(row));
                        price = (int)amount;
                        category = products.GetValueOrDefault(keys.Encode(fields[5].Get(row)));
                    }
                }
            }
            // Invalid candidates still participate in deduplication as tombstones.
            int shard = PersonShard(person);
            int index = -1;
            if (person != 0)
            {
                var events = buffers.People[shard];
                index = events.Count;
                events.Add(new Event(person, time, price, category));
            }
            buffers.Ids[Shard(id)].Add(new Candidate(id, ingest, shard, index));
        }
    }

    private IList<List<Click>[]> LoadClicks()
    {
        using var clicks = new ThreadLocal<List<Click>[]>(Buckets<Click>, trackAllValues: true);
        Input.ReadFiles(Files("clicks"), parallel, (chunk, _) =>
        {
            var buckets = clicks.Value!;
            Span<Field> fields = stackalloc Field[4];
            while (!chunk.IsEmpty)
            {
                if (!Input.ReadRow(ref chunk, fields, out var row) || fields[0].Get(row).IsEmpty || fields[1].Get(row).IsEmpty ||
                    !campaignIds.TryGetValue(keys.Encode(fields[2].Get(row)), out int campaign))
                    continue;
                long time = Input.Timestamp(fields[3].Get(row));
                if (time < start - Input.Week || time >= end) continue;
                ulong person = keys.Encode(fields[1].Get(row));
                // Identical click copies leave the winning click and the set of assists unchanged.
                buckets[PersonShard(person)].Add(new Click(person, time, keys.Encode(fields[0].Get(row)), campaign));
            }
        });
        return clicks.Values;
    }

    public void Run()
    {
        LoadDimensions();
        RouteLinks();
        var eventBuckets = LoadEvents();
        var clickBuckets = LoadClicks();
        var totals = new Dictionary<(int Campaign, int Day), CampaignDay>[ShardCount];
        Directory.CreateDirectory(Path.Combine(output, "user_daily"));
        Directory.CreateDirectory(Path.Combine(output, "campaign_daily"));

        Parallel.For(0, ShardCount, parallel, shard =>
        {
            var edges = linkParts[shard];
            var links = new Links(edges.Count * 2);
            foreach (var edge in edges) links.Join(edge.A, edge.B, keys);
            linkParts[shard] = null!;
            var (ids, names) = links.Finish();
            var (events, purchases) = GroupEvents(eventBuckets, shard, ids, names);
            var clicks = Group(clickBuckets, shard, new ClickOrder(keys), ids, names);
            var counts = new Dictionary<(int Campaign, int Day), CampaignDay>();
            totals[shard] = counts;
            var users = Sessionize(events, purchases, clicks, names, counts);
            if (users.Count > 0 || shard == 0)
                WriteUsers(Path.Combine(output, "user_daily", $"part-{shard:D2}.parquet"), users);
        });

        var combined = new Dictionary<(int Campaign, int Day), CampaignDay>();
        foreach (var shard in totals)
            foreach (var (key, row) in shard)
                Accumulate(combined, key, row.Sessions, row.Purchases, row.Revenue, row.Assists);
        WriteCampaigns(Path.Combine(output, "campaign_daily", "part-00.parquet"), combined.Values.ToArray());
    }

    private interface IPersonOrder<T> : IComparer<T>
    {
        ulong Person(T row);
    }

    private readonly struct PurchaseOrder : IComparer<Purchase>
    {
        public int Compare(Purchase a, Purchase b) => a.Time.CompareTo(b.Time);
    }

    private readonly struct ClickOrder(Keys keys) : IPersonOrder<Click>
    {
        public ulong Person(Click row) => row.Person;
        public int Compare(Click a, Click b)
        {
            if (a.Time != b.Time) return a.Time.CompareTo(b.Time);
            return keys.Compare(b.Id, a.Id);
        }
    }



    private readonly record struct Groups<T>(T[] Rows, int[] Ends)
    {
        public ArraySegment<T> Segment(int group)
        {
            if (group >= Ends.Length) return ArraySegment<T>.Empty;
            int start = group == 0 ? 0 : Ends[group - 1];
            return new ArraySegment<T>(Rows, start, Ends[group] - start);
        }
    }

    private static (Groups<long>, Groups<Purchase>) GroupEvents(IList<List<Event>[]> sources, int shard,
        Dictionary<ulong, int> people, List<ulong> names)
    {
        var groupIds = GC.AllocateUninitializedArray<int>(sources.Sum(s => s[shard].Count));
        var eventCounts = new List<int>(names.Count);
        var purchaseCounts = new List<int>(names.Count);
        CollectionsMarshal.SetCount(eventCounts, names.Count);
        CollectionsMarshal.SetCount(purchaseCounts, names.Count);
        int position = 0;
        foreach (var source in sources)
            foreach (ref readonly var row in CollectionsMarshal.AsSpan(source[shard]))
            {
                if (row.Person == 0) { groupIds[position++] = -1; continue; }
                ref int group = ref CollectionsMarshal.GetValueRefOrAddDefault(people, row.Person, out bool found);
                if (!found)
                {
                    group = eventCounts.Count;
                    names.Add(row.Person);
                    eventCounts.Add(0);
                    purchaseCounts.Add(0);
                }
                CollectionsMarshal.AsSpan(eventCounts)[group]++;
                if (row.Price != 0) CollectionsMarshal.AsSpan(purchaseCounts)[group]++;
                groupIds[position++] = group;
            }
        var times = AllocateGroups<long>(eventCounts);
        var purchases = AllocateGroups<Purchase>(purchaseCounts);
        position = 0;
        foreach (var source in sources)
        {
            foreach (ref readonly var row in CollectionsMarshal.AsSpan(source[shard]))
            {
                int group = groupIds[position++];
                if (group < 0) continue;
                times.Rows[times.Ends[group]++] = row.Time;
                if (row.Price != 0)
                    purchases.Rows[purchases.Ends[group]++] = new Purchase(row.Time, row.Price, row.Category);
            }
            source[shard] = null!;
        }
        return (times, purchases);
    }

    // Each start offset advances during scatter and becomes the exclusive end.
    private static Groups<T> AllocateGroups<T>(List<int> counts)
    {
        int total = 0;
        var offsets = new int[counts.Count];
        for (int group = 0; group < offsets.Length; group++)
        {
            offsets[group] = total;
            total += counts[group];
        }
        return new Groups<T>(GC.AllocateUninitializedArray<T>(total), offsets);
    }

    private static Groups<T> Group<T, TOrder>(IEnumerable<List<T>[]> sources, int shard,
        TOrder order, Dictionary<ulong, int> people, List<ulong> names) where TOrder : struct, IPersonOrder<T>
    {
        int count = sources.Sum(s => s[shard].Count);
        var counts = new List<int>(names.Count);
        CollectionsMarshal.SetCount(counts, names.Count);
        var groups = GC.AllocateUninitializedArray<int>(count);
        int position = 0;
        foreach (var source in sources)
        {
            foreach (var row in CollectionsMarshal.AsSpan(source[shard]))
            {
                ulong person = order.Person(row);
                int group;
                if (person == 0) group = -1;
                else
                {
                    ref int entry = ref CollectionsMarshal.GetValueRefOrAddDefault(people, person, out bool exists);
                    if (!exists)
                    {
                        entry = counts.Count;
                        names.Add(person);
                        counts.Add(0);
                    }
                    group = entry;
                }
                groups[position++] = group;
                if (group >= 0) CollectionsMarshal.AsSpan(counts)[group]++;
            }
        }

        var result = AllocateGroups<T>(counts);
        position = 0;
        foreach (var source in sources)
        {
            foreach (var row in CollectionsMarshal.AsSpan(source[shard]))
            {
                int group = groups[position++];
                if (group >= 0) result.Rows[result.Ends[group]++] = row;
            }
            source[shard] = null!;
        }
        return result;
    }

    private List<UserDay> Sessionize(Groups<long> eventGroups, Groups<Purchase> purchaseGroups, Groups<Click> clickGroups, List<ulong> names,
        Dictionary<(int Campaign, int Day), CampaignDay> totals)
    {
        var users = new List<UserDay>();
        var categoryRevenue = new long[categories.Count];
        var usedCategories = new List<int>();
        var active = new Dictionary<int, int>();
        var clickOrder = new ClickOrder(keys);
        for (int group = 0; group < eventGroups.Ends.Length; group++)
        {
            var events = eventGroups.Segment(group).AsSpan();
            if (events.IsEmpty) continue;
            var purchases = purchaseGroups.Segment(group).AsSpan();
            var clicks = clickGroups.Segment(group).AsSpan();
            ulong person = names[group];
            int eventEnd = events.Length, purchaseEnd = purchases.Length, clickEnd = clicks.Length;
            events.Sort();
            purchases.Sort(new PurchaseOrder());
            clicks.Sort(clickOrder);
            int i = 0, first = 0, next = 0, purchase = 0;
            var initial = events[0];
            int day = Input.EpochDay(initial);
            long userSessions = 0, userEvents = 0, userRevenue = 0;

            active.Clear();
            while (i < eventEnd)
            {
                long sessionStart = events[i];
                int sessionDay = Input.EpochDay(sessionStart);
                if (sessionDay != day)
                {
                    FinishDay();
                    day = sessionDay;
                }

                while (first < next && clicks[first].Time < sessionStart - Input.Week)
                {
                    int campaign = clicks[first++].Campaign;
                    if (--active[campaign] == 0) active.Remove(campaign);
                }
                while (next < clickEnd && clicks[next].Time <= sessionStart)
                {
                    var click = clicks[next++];
                    if (click.Time >= sessionStart - Input.Week)
                        active[click.Campaign] = active.GetValueOrDefault(click.Campaign) + 1;
                    else
                        first = next;
                }

                long revenue = 0;
                int from = i;
                i++;
                while (i < eventEnd && events[i] - events[i - 1] <= 1_800_000) i++;
                long last = events[i - 1];
                while (purchase < purchaseEnd && purchases[purchase].Time <= last)
                {
                    var row = purchases[purchase++];
                    if (categoryRevenue[row.Category] == 0) usedCategories.Add(row.Category);
                    categoryRevenue[row.Category] += row.Price;
                    revenue += row.Price;
                }

                int credited = first < next ? clicks[next - 1].Campaign : 0;
                Accumulate(totals, (credited, day), 1, revenue > 0 ? 1 : 0, revenue, 0);
                foreach (int campaign in active.Keys)
                    Accumulate(totals, (campaign, day), 0, 0, 0, 1);
                userSessions++;
                userEvents += i - from;
                userRevenue += revenue;
            }
            FinishDay();

            void FinishDay()
            {
                int best = -1;
                long max = 0;
                foreach (int category in usedCategories)
                {
                    long revenue = categoryRevenue[category];
                    if (revenue > max || (revenue == max && keys.Compare(categories[category], categories[best]) < 0))
                    {
                        best = category;
                        max = revenue;
                    }
                    categoryRevenue[category] = 0;
                }
                usedCategories.Clear();
                users.Add(new UserDay(person, day, userSessions, userEvents, userRevenue, best));
                userSessions = userEvents = userRevenue = 0;
            }
        }
        return users;
    }

    private static void Accumulate(Dictionary<(int Campaign, int Day), CampaignDay> totals,
        (int Campaign, int Day) key, long sessions, long purchases, long revenue, long assists)
    {
        ref var value = ref CollectionsMarshal.GetValueRefOrAddDefault(totals, key, out _);
        value = new CampaignDay(key.Campaign, key.Day, value.Sessions + sessions,
            value.Purchases + purchases, value.Revenue + revenue, value.Assists + assists);
    }

    private void WriteUsers(string path, List<UserDay> rows) =>
        Parquet.Write(path, rows, keys,
            Parquet.Text<UserDay>("user_id", r => r.Person),
            Parquet.Date<UserDay>("day", r => r.Day),
            Parquet.Integer<UserDay>("sessions", r => r.Sessions),
            Parquet.Integer<UserDay>("events", r => r.Events),
            Parquet.Integer<UserDay>("revenue_cents", r => r.Revenue),
            Parquet.Text<UserDay>("top_category", r => r.Category < 0 ? null : categories[r.Category], optional: true));

    private void WriteCampaigns(string path, CampaignDay[] rows) =>
        Parquet.Write(path, rows, keys,
            Parquet.Text<CampaignDay>("campaign_id", r => campaigns[r.Campaign].Id),
            Parquet.Text<CampaignDay>("channel", r => campaigns[r.Campaign].Channel),
            Parquet.Date<CampaignDay>("day", r => r.Day),
            Parquet.Integer<CampaignDay>("sessions", r => r.Sessions),
            Parquet.Integer<CampaignDay>("purchasing_sessions", r => r.Purchases),
            Parquet.Integer<CampaignDay>("revenue_cents", r => r.Revenue),
            Parquet.Integer<CampaignDay>("assisted_sessions", r => r.Assists));
}

static class Program
{
    public static void Main(string[] args)
    {
        var data = args.Length > 0 ? args[0] : Environment.GetEnvironmentVariable("DATA_DIR") ?? "/data";
        var output = args.Length > 1 ? args[1] : Environment.GetEnvironmentVariable("OUTPUT_DIR") ?? "/output";
        int cpus = int.TryParse(Environment.GetEnvironmentVariable("NCPU"), out int n) && n > 0
            ? n : Environment.ProcessorCount;
        new Pipeline(data, output, cpus).Run();
    }
}
