using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;

readonly record struct RawEvent(ulong Id, ulong Person, long Time, long Ingest, int Price, int Category);
readonly record struct Event(ulong Person, long Time, int Price, int Category);
readonly record struct Click(ulong Person, long Time, ulong Id, int Campaign);
readonly record struct UserDay(ulong Person, int Day, long Sessions, long Events, long Revenue, int Category);
readonly record struct CampaignDay(int Campaign, int Day, long Sessions, long Purchases, long Revenue, long Assists);

sealed class Links
{
    private readonly Dictionary<ulong, int> index = [];
    private readonly List<int> parent = [], sizes = [];
    private readonly List<ulong> minimum = [];

    private int Add(ulong key)
    {
        if (index.TryGetValue(key, out int i)) return i;
        i = parent.Count;
        index.Add(key, i);
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
        if (keys.Compare(minimum[right], minimum[left]) < 0)
            minimum[left] = minimum[right];
    }

    public Dictionary<ulong, ulong> Finish()
    {
        var result = new Dictionary<ulong, ulong>(index.Count);
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
    private Dictionary<ulong, ulong> people = null!;
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
            Span<Range> fields = stackalloc Range[2];
            while (!chunk.IsEmpty)
            {
                var row = Input.Line(ref chunk);
                if (Input.Fields(row, fields)) use(row[fields[0]], row[fields[1]]);
            }
        });
    }

    private void LoadDimensions()
    {
        using var manifest = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(data, "manifest.json")));
        start = manifest.RootElement.GetProperty("window_start").GetDateTimeOffset().ToUnixTimeMilliseconds();
        end = manifest.RootElement.GetProperty("window_end").GetDateTimeOffset().ToUnixTimeMilliseconds();

        var links = new Links();
        Pairs(Files("links"), (a, b) =>
        {
            if (!a.IsEmpty && !b.IsEmpty) links.Join(keys.Encode(a), keys.Encode(b), keys);
        });
        people = links.Finish();

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong Person(ReadOnlySpan<byte> field)
    {
        ulong key = keys.Encode(field);
        return people.GetValueOrDefault(key, key);
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

    private static List<T>[] Buckets<T>() =>
        Enumerable.Range(0, ShardCount).Select(_ => new List<T>()).ToArray();

    private List<Event>[][] LoadEvents()
    {
        var outputBuckets = new List<Event>[ShardCount][];
        using var raw = new ThreadLocal<List<RawEvent>[]>(Buckets<RawEvent>, trackAllValues: true);
        Input.ReadFiles(Files("events"), parallel, (chunk, _) => ParseEvents(chunk, raw.Value!));
        var inputs = raw.Values;

        Parallel.For(0, ShardCount, parallel, shard =>
        {
            var buckets = Buckets<Event>();
            outputBuckets[shard] = buckets;
            int count = inputs.Sum(source => source[shard].Count);
            if (count == 0) return;

            var unique = new RawEvent[count];
            var slots = new int[BitOperations.RoundUpToPowerOf2((uint)(count + count / 2 + 1))];
            int used = 0;
            foreach (var source in inputs)
            {
                foreach (var row in source[shard])
                {
                    int slot = (int)((Hash(row.Id) >> 6) & (uint)(slots.Length - 1));
                    while (slots[slot] != 0 && unique[slots[slot] - 1].Id != row.Id)
                        slot = (slot + 1) & (slots.Length - 1);
                    int current = slots[slot] - 1;
                    if (current < 0)
                    {
                        unique[used] = row;
                        slots[slot] = ++used;
                    }
                    else if (row.Ingest < unique[current].Ingest)
                        unique[current] = row;
                }
                source[shard] = null!;
            }
            foreach (ref readonly var row in unique.AsSpan(0, used))
                if (row.Person != 0)
                    buckets[Shard(row.Person)].Add(new Event(row.Person, row.Time, row.Price, row.Category));
        });
        return outputBuckets;
    }

    private void ParseEvents(ReadOnlySpan<byte> chunk, List<RawEvent>[] buckets)
    {
        Span<Range> fields = stackalloc Range[7];
        while (!chunk.IsEmpty)
        {
            var row = Input.Line(ref chunk);
            if (!Input.Fields(row, fields) || row[fields[0]].IsEmpty) continue;
            ulong id = keys.Encode(row[fields[0]]);
            long ingest = Input.IngestTimestamp(row[fields[3]]);
            long time = Input.Timestamp(row[fields[2]]);
            ulong person = 0;
            int price = 0, category = 0;

            if (time >= start && time < end && ingest - time <= Input.Day &&
                !row[fields[1]].IsEmpty && !row[fields[5]].IsEmpty)
            {
                var type = row[fields[4]];
                if (type.SequenceEqual("view"u8) || type.SequenceEqual("cart"u8))
                    person = Person(row[fields[1]]);
                else if (type.SequenceEqual("purchase"u8))
                {
                    long amount = Input.Number(row[fields[6]]);
                    if (amount is >= 1 and <= 10_000_000)
                    {
                        person = Person(row[fields[1]]);
                        price = (int)amount;
                        category = products.GetValueOrDefault(keys.Encode(row[fields[5]]));
                    }
                }
            }
            // Invalid candidates still participate in deduplication as tombstones.
            buckets[Shard(id)].Add(new RawEvent(id, person, time, ingest, price, category));
        }
    }

    private IList<List<Click>[]> LoadClicks()
    {
        using var clicks = new ThreadLocal<List<Click>[]>(Buckets<Click>, trackAllValues: true);
        Input.ReadFiles(Files("clicks"), parallel, (chunk, _) =>
        {
            var buckets = clicks.Value!;
            Span<Range> fields = stackalloc Range[4];
            while (!chunk.IsEmpty)
            {
                var row = Input.Line(ref chunk);
                if (!Input.Fields(row, fields) || row[fields[0]].IsEmpty || row[fields[1]].IsEmpty ||
                    !campaignIds.TryGetValue(keys.Encode(row[fields[2]]), out int campaign))
                    continue;
                long time = Input.Timestamp(row[fields[3]]);
                if (time < start - Input.Week || time >= end) continue;
                ulong person = Person(row[fields[1]]);
                // Identical click copies leave the winning click and the set of assists unchanged.
                buckets[Shard(person)].Add(new Click(person, time, keys.Encode(row[fields[0]]), campaign));
            }
        });
        return clicks.Values;
    }

    public void Run()
    {
        LoadDimensions();
        var eventBuckets = LoadEvents();
        var clickBuckets = LoadClicks();
        var totals = new Dictionary<(int Campaign, int Day), CampaignDay>[ShardCount];
        Directory.CreateDirectory(Path.Combine(output, "user_daily"));
        Directory.CreateDirectory(Path.Combine(output, "campaign_daily"));

        Parallel.For(0, ShardCount, parallel, shard =>
        {
            var events = Combine(eventBuckets, shard);
            var clicks = Combine(clickBuckets, shard);
            CollectionsMarshal.AsSpan(events).Sort(new EventOrder());
            CollectionsMarshal.AsSpan(clicks).Sort(new ClickOrder(keys));

            var counts = new Dictionary<(int Campaign, int Day), CampaignDay>();
            totals[shard] = counts;
            var users = Sessionize(events, clicks, counts);
            if (users.Count > 0 || shard == 0)
                WriteUsers(Path.Combine(output, "user_daily", $"part-{shard:D2}.parquet"), users);
        });

        var combined = new Dictionary<(int Campaign, int Day), CampaignDay>();
        foreach (var shard in totals)
            foreach (var (key, row) in shard)
                Accumulate(combined, key, row.Sessions, row.Purchases, row.Revenue, row.Assists);
        WriteCampaigns(Path.Combine(output, "campaign_daily", "part-00.parquet"), combined.Values.ToArray());
    }

    private readonly struct EventOrder : IComparer<Event>
    {
        public int Compare(Event a, Event b) =>
            a.Person == b.Person ? a.Time.CompareTo(b.Time) : a.Person.CompareTo(b.Person);
    }

    private readonly struct ClickOrder(Keys keys) : IComparer<Click>
    {
        public int Compare(Click a, Click b)
        {
            if (a.Person != b.Person) return a.Person.CompareTo(b.Person);
            if (a.Time != b.Time) return a.Time.CompareTo(b.Time);
            // The sweep takes the final eligible click, so the smallest ID sorts last on a tie.
            return keys.Compare(b.Id, a.Id);
        }
    }

    private static List<T> Combine<T>(IEnumerable<List<T>[]> sources, int shard)
    {
        var rows = new List<T>(sources.Sum(source => source[shard].Count));
        foreach (var source in sources)
        {
            rows.AddRange(source[shard]);
            source[shard] = null!;
        }
        return rows;
    }

    private List<UserDay> Sessionize(List<Event> events, List<Click> clicks,
        Dictionary<(int Campaign, int Day), CampaignDay> totals)
    {
        var users = new List<UserDay>();
        var categoryRevenue = new long[categories.Count];
        var usedCategories = new List<int>();
        var active = new Dictionary<int, int>();
        int i = 0, first = 0, next = 0;

        while (i < events.Count)
        {
            var initial = events[i];
            ulong person = initial.Person;
            int day = Input.EpochDay(initial.Time);
            long userSessions = 0, userEvents = 0, userRevenue = 0;

            while (next < clicks.Count && clicks[next].Person < person) next++;
            first = next;
            active.Clear();
            while (i < events.Count && events[i].Person == person)
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
                while (next < clicks.Count && clicks[next].Person == person && clicks[next].Time <= sessionStart)
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
                while (i < events.Count && events[i].Person == person && events[i].Time - previous <= 1_800_000);

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



