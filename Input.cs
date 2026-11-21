using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Text;

static class Input
{
    public const long Day = 86_400_000;
    public const long Week = 7 * Day;
    private static readonly int[] MonthStarts = MakeMonths();

    private static int[] MakeMonths()
    {
        var starts = new int[120_000];
        int epoch = DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber;
        for (int year = 1; year < 10_000; year++)
            for (int month = 1; month <= 12; month++)
                starts[year * 12 + month - 1] = new DateOnly(year, month, 1).DayNumber - epoch;
        return starts;
    }

    public static long Number(ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty) return long.MinValue;
        long number = 0;
        foreach (byte b in value)
        {
            uint digit = (uint)(b - '0');
            if (digit > 9) return long.MinValue;
            if (number > long.MaxValue / 10 || (number == long.MaxValue / 10 && digit > 7))
                return long.MinValue;
            number = number * 10 + digit;
        }
        return number;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Two(ReadOnlySpan<byte> value, int offset)
    {
        int hi = value[offset] - '0';
        int lo = value[offset + 1] - '0';
        return (uint)hi <= 9 && (uint)lo <= 9 ? hi * 10 + lo : -1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Digits(ReadOnlySpan<byte> value, int offset) =>
        (value[offset] - '0') * 10 + value[offset + 1] - '0';

    // Ingest timestamps are guaranteed to be valid whole-second UTC values.
    public static long IngestTimestamp(ReadOnlySpan<byte> value)
    {
        int year = Digits(value, 0) * 100 + Digits(value, 2);
        int day = MonthStarts[year * 12 + Digits(value, 5) - 1] + Digits(value, 8) - 1;
        return day * Day + ((Digits(value, 11) * 60L + Digits(value, 14)) * 60 + Digits(value, 17)) * 1000;
    }

    public static long Timestamp(ReadOnlySpan<byte> value)
    {
        if (value.Length >= 5 && value[4] == '-')
            return CalendarTimestamp(value);
        return Number(value);
    }

    public static long CalendarTimestamp(ReadOnlySpan<byte> value)
    {
        if ((value.Length != 20 && value.Length != 25) || value[4] != '-' ||
            value[7] != '-' || value[10] != 'T' || value[13] != ':' || value[16] != ':')
            return long.MinValue;

        int century = Two(value, 0), yearOfCentury = Two(value, 2);
        int month = Two(value, 5), day = Two(value, 8);
        int hour = Two(value, 11), minute = Two(value, 14), second = Two(value, 17);
        int year = century * 100 + yearOfCentury;
        if (century < 0 || yearOfCentury < 0 || year == 0 || month is < 1 or > 12 ||
            day is < 1 or > 31 || (uint)hour > 23 || (uint)minute > 59 || (uint)second > 59)
            return long.MinValue;

        if (day > 28 && day > DateTime.DaysInMonth(year, month))
            return long.MinValue;

        int offset = 0;
        if (value.Length == 20)
        {
            if (value[19] != 'Z') return long.MinValue;
        }
        else
        {
            if (value[19] is not ((byte)'+') and not ((byte)'-') || value[22] != ':')
                return long.MinValue;
            int hours = Two(value, 20), minutes = Two(value, 23);
            if ((uint)hours > 23 || (uint)minutes > 59) return long.MinValue;
            offset = (hours * 60 + minutes) * (value[19] == '+' ? 1 : -1);
        }
        return (MonthStarts[year * 12 + month - 1] + day - 1) * Day +
            ((hour * 60L + minute - offset) * 60 + second) * 1000;
    }

    public static bool Fields(ReadOnlySpan<byte> line, Span<Range> fields)
    {
        int start = 0;
        for (int i = 0; i < fields.Length; i++)
        {
            int comma = line[start..].IndexOf((byte)',');
            bool last = i == fields.Length - 1;
            if (last != (comma < 0)) return false;
            int end = last ? line.Length : start + comma;
            int next = end + 1;
            if (start < end && line[start] == '"')
            {
                if (end - start < 2 || line[end - 1] != '"') return false;
                start++;
                end--;
            }
            if (line[start..end].Contains((byte)'"')) return false;
            fields[i] = start..end;
            start = next;
        }
        return true;
    }

    public delegate void ReadChunk(ReadOnlySpan<byte> content, int chunk);

    public static unsafe void ReadFiles(IEnumerable<string> files, ParallelOptions options, ReadChunk read)
    {
        const int chunkSize = 8 << 20;
        var maps = new List<(MemoryMappedFile File, MemoryMappedViewAccessor View)>();
        var chunks = new List<(MemoryMappedViewAccessor View, long Start, long End, long Length)>();
        try
        {
            foreach (string path in files)
            {
                long length = new FileInfo(path).Length;
                if (length == 0) continue;
                var map = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
                var view = map.CreateViewAccessor(0, length, MemoryMappedFileAccess.Read);
                maps.Add((map, view));
                for (long start = 0; start < length; start += chunkSize)
                    chunks.Add((view, start, Math.Min(start + chunkSize, length), length));
            }

            Parallel.For(0, chunks.Count, options, index =>
            {
                var (view, start, end, length) = chunks[index];
                byte* ptr = null;
                view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                try
                {
                    ptr += view.PointerOffset;
                    // A chunk starts after the next LF unless it already begins on a row.
                    // For the first chunk, this also drops the file header.
                    if (start == 0 || ptr[start - 1] != '\n')
                        start = NextLine(ptr, start, length);
                    if (end < length && ptr[end - 1] != '\n')
                        end = NextLine(ptr, end, length);
                    if (start < end)
                        read(new ReadOnlySpan<byte>(ptr + start, checked((int)(end - start))), index);
                }
                finally { view.SafeMemoryMappedViewHandle.ReleasePointer(); }
            });
        }
        finally
        {
            foreach (var (file, view) in maps)
            {
                view.Dispose();
                file.Dispose();
            }
        }
    }

    private static unsafe long NextLine(byte* ptr, long start, long length)
    {
        while (start < length)
        {
            int size = (int)Math.Min(8 << 20, length - start);
            int lf = new ReadOnlySpan<byte>(ptr + start, size).IndexOf((byte)'\n');
            if (lf >= 0) return start + lf + 1;
            start += size;
        }
        return length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<byte> Line(ref ReadOnlySpan<byte> chunk)
    {
        int lf = chunk.IndexOf((byte)'\n');
        int size = lf < 0 ? chunk.Length : lf;
        var line = chunk[..size];
        chunk = lf < 0 ? default : chunk[(lf + 1)..];
        return line;
    }

    public static int EpochDay(long timestamp) =>
        (int)(timestamp >= 0 ? timestamp / Day : (timestamp + 1) / Day - 1);
}

sealed class Keys
{
    private readonly Dictionary<string, ulong> ids = new(StringComparer.Ordinal);
    private readonly List<byte[]> values = [];

    // Nine 7-bit ASCII characters fit inline, in byte sort order. Zero pads
    // shorter keys, and the high bit marks keys in the extended string table.
    private const ulong Characters = 0x7f7f7f7f7f7f7f7fUL;
    private const ulong ExtendedBit = 1UL << 63;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong Encode(ReadOnlySpan<byte> bytes)
    {
        if ((uint)(bytes.Length - 8) <= 1 && Bmi2.X64.IsSupported)
        {
            ulong value = BinaryPrimitives.ReadUInt64BigEndian(bytes);
            ulong last = bytes.Length == 9 ? bytes[8] : 0UL;
            if (((value | (value - 0x0101010101010101UL)) & ~Characters) == 0 &&
                (bytes.Length == 8 || last is > 0 and < 128))
                return (Bmi2.X64.ParallelBitExtract(value, Characters) << 7) | last;
        }
        else if (bytes.Length <= 9)
        {
            ulong value = 0;
            foreach (byte b in bytes)
            {
                if (b is 0 or >= 128) return Extended(bytes);
                value = (value << 7) | b;
            }
            return value << ((9 - bytes.Length) * 7);
        }
        return Extended(bytes);
    }

    private ulong Extended(ReadOnlySpan<byte> bytes)
    {
        string text = Encoding.UTF8.GetString(bytes);
        lock (ids)
        {
            if (ids.TryGetValue(text, out ulong id)) return id;
            id = ExtendedBit | (uint)values.Count;
            ids.Add(text, id);
            values.Add(bytes.ToArray());
            return id;
        }
    }

    public int Compare(ulong left, ulong right)
    {
        if ((left | right) < ExtendedBit) return left.CompareTo(right);
        Span<byte> l = stackalloc byte[9], r = stackalloc byte[9];
        return Bytes(left, l).SequenceCompareTo(Bytes(right, r));
    }

    public ReadOnlySpan<byte> Bytes(ulong key, Span<byte> buffer)
    {
        if (key >= ExtendedBit) return values[(int)(uint)key];
        if (Bmi2.X64.IsSupported)
        {
            BinaryPrimitives.WriteUInt64BigEndian(buffer,
                Bmi2.X64.ParallelBitDeposit(key >> 7, Characters));
            buffer[8] = (byte)(key & 127);
        }
        else
        {
            for (int i = 0; i < 9; i++)
                buffer[i] = (byte)((key >> ((8 - i) * 7)) & 127);
        }
        return buffer[..(key == 0 ? 0 : 9 - BitOperations.TrailingZeroCount(key) / 7)];
    }
}



