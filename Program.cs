using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;

readonly record struct Candidate(ulong Id, long Ingest, int Shard, int Index);
readonly record struct Event(ulong Person, long Time, int Price, int Category);
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

    public Dictionary<ulong, ulong> Finish()
    {
        var result = new Dictionary<ulong, ulong>(index.Count - sizes.Count(n => n > 0));
        foreach (var (key, i) in index)
        {
            ulong person = minimum[Root(i)];
            if (key != person) result.Add(key, person);
        }
        return result;
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

    private Dictionary<ulong, ulong> LoadLinks()
    {
        var files = Files("links").ToArray();
        int estimate = (int)Math.Min(8_000_000, files.Sum(path => new FileInfo(path).Length) / 12);
        var links = new Links(estimate);
        Pairs(files, (a, b) =>
        {
            if (!a.IsEmpty && !b.IsEmpty) links.Join(keys.Encode(a), keys.Encode(b), keys);
        });
        return links.Finish();
    }

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

    private IList<List<Event>[]> LoadEvents(Action loadLinks)
    {
        var files = Files("events").ToArray();
        long bytes = files.Sum(path => new FileInfo(path).Length);
        int capacity = (int)Math.Min(262144, bytes / (72L * cpus * ShardCount));
        using var local = new ThreadLocal<EventBuffers>(() => new EventBuffers(capacity), true);
        Input.ReadFiles(files, parallel, (chunk, _) => ParseEvents(chunk, local.Value!), loadLinks);
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
            int shard = Shard(person);
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
                buckets[Shard(person)].Add(new Click(person, time, keys.Encode(fields[0].Get(row)), campaign));
            }
        });
        return clicks.Values;
    }

    public void Run()
    {
        LoadDimensions();
        Dictionary<ulong, ulong> links = null!;
        var eventBuckets = LoadEvents(() => links = LoadLinks());
        var clickBuckets = LoadClicks();
        var totals = new Dictionary<(int Campaign, int Day), CampaignDay>[ShardCount];
        var grouped = new List<AliasRows>[ShardCount][];
        Directory.CreateDirectory(Path.Combine(output, "user_daily"));
        Directory.CreateDirectory(Path.Combine(output, "campaign_daily"));

        Parallel.For(0, ShardCount, parallel, shard =>
        {
            var rawIds = new Dictionary<ulong, int>();
            var events = Group(eventBuckets, shard, new EventOrder(), rawIds);
            var clicks = Group(clickBuckets, shard, new ClickOrder(keys), rawIds);
            if (links.Count == 0)
            {
                var rows = new List<AliasRows>(rawIds.Count);
                foreach (var (id, index) in rawIds)
                    rows.Add(new AliasRows(id, events.Segment(index), clicks.Segment(index)));
                WriteShard(shard, rows);
                return;
            }

            var partitions = Buckets<AliasRows>();
            grouped[shard] = partitions;
            foreach (var (id, index) in rawIds)
            {
                ulong person = links.GetValueOrDefault(id, id);
                partitions[Shard(person)].Add(new AliasRows(person, events.Segment(index), clicks.Segment(index)));
            }
        });

        if (links.Count > 0)
        {
            Parallel.For(0, ShardCount, parallel, shard =>
            {
                var aliases = new List<AliasRows>(grouped.Sum(parts => parts[shard].Count));
                foreach (var parts in grouped)
                {
                    aliases.AddRange(parts[shard]);
                    parts[shard] = null!;
                }
                aliases.Sort(static (a, b) => a.Person.CompareTo(b.Person));
                WriteShard(shard, aliases);
            });
        }

        void WriteShard(int shard, List<AliasRows> aliases)
        {
            var counts = new Dictionary<(int Campaign, int Day), CampaignDay>();
            totals[shard] = counts;
            var users = Sessionize(aliases, counts);
            if (users.Count > 0 || shard == 0)
                WriteUsers(Path.Combine(output, "user_daily", $"part-{shard:D2}.parquet"), users);
        }

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

    private readonly struct EventOrder : IPersonOrder<Event>
    {
        public ulong Person(Event row) => row.Person;
        public int Compare(Event a, Event b) => a.Time.CompareTo(b.Time);
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

    private readonly record struct AliasRows(ulong Person, ArraySegment<Event> Events, ArraySegment<Click> Clicks);

    private readonly record struct Groups<T>(T[] Rows, int[] Ends)
    {
        public ArraySegment<T> Segment(int group)
        {
            if (group >= Ends.Length) return ArraySegment<T>.Empty;
            int start = group == 0 ? 0 : Ends[group - 1];
            return new ArraySegment<T>(Rows, start, Ends[group] - start);
        }
    }

    private static Groups<T> Group<T, TOrder>(IEnumerable<List<T>[]> sources, int shard,
        TOrder order, Dictionary<ulong, int> people) where TOrder : struct, IPersonOrder<T>
    {
        int count = sources.Sum(s => s[shard].Count);
        var counts = new List<int>(people.Count);
        CollectionsMarshal.SetCount(counts, people.Count);
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
                        counts.Add(0);
                    }
                    group = entry;
                }
                groups[position++] = group;
                if (group >= 0) CollectionsMarshal.AsSpan(counts)[group]++;
            }
        }

        var offsets = new int[counts.Count];
        position = 0;
        for (int group = 0; group < counts.Count; group++)
        {
            offsets[group] = position;
            position += counts[group];
        }

        var rows = GC.AllocateUninitializedArray<T>(position);
        position = 0;
        foreach (var source in sources)
        {
            foreach (var row in CollectionsMarshal.AsSpan(source[shard]))
            {
                int group = groups[position++];
                if (group >= 0) rows[offsets[group]++] = row;
            }
            source[shard] = null!;
        }
        return new Groups<T>(rows, offsets);
    }

    private List<UserDay> Sessionize(List<AliasRows> aliases,
        Dictionary<(int Campaign, int Day), CampaignDay> totals)
    {
        Event[] eventScratch = [];
        Click[] clickScratch = [];
        var users = new List<UserDay>();
        var categoryRevenue = new long[categories.Count];
        var usedCategories = new List<int>();
        var active = new Dictionary<int, int>();
        int group = 0;
        var clickOrder = new ClickOrder(keys);
        while (group < aliases.Count)
        {
            int begin = group;
            var alias = aliases[group++];
            ulong person = alias.Person;
            int eventEnd = alias.Events.Count, clickEnd = alias.Clicks.Count;
            while (group < aliases.Count && aliases[group].Person == person)
            {
                eventEnd += aliases[group].Events.Count;
                clickEnd += aliases[group++].Clicks.Count;
            }
            if (eventEnd == 0) continue;
            Span<Event> events = alias.Events.AsSpan();
            Span<Click> clicks = alias.Clicks.AsSpan();
            if (group - begin > 1)
            {
                if (eventScratch.Length < eventEnd) eventScratch = new Event[eventEnd];
                if (clickScratch.Length < clickEnd) clickScratch = new Click[clickEnd];
                events = eventScratch.AsSpan(0, eventEnd);
                clicks = clickScratch.AsSpan(0, clickEnd);
                int e = 0, c = 0;
                for (int j = begin; j < group; j++)
                {
                    var part = aliases[j];
                    part.Events.AsSpan().CopyTo(events[e..]);
                    part.Clicks.AsSpan().CopyTo(clicks[c..]);
                    e += part.Events.Count;
                    c += part.Clicks.Count;
                }
            }
            events.Sort(new EventOrder());
            clicks.Sort(clickOrder);
            int i = 0, first = 0, next = 0;
            var initial = events[0];
            int day = Input.EpochDay(initial.Time);
            long userSessions = 0, userEvents = 0, userRevenue = 0;

            active.Clear();
            while (i < eventEnd)
            {
                long sessionStart = events[i].Time;
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
                long previous;
                do
                {
                    var row = events[i++];
                    previous = row.Time;
                    if (row.Price > 0)
                    {
                        if (categoryRevenue[row.Category] == 0) usedCategories.Add(row.Category);
                        categoryRevenue[row.Category] += row.Price;
                        revenue += row.Price;
                    }
                }
                while (i < eventEnd && events[i].Time - previous <= 1_800_000);

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
