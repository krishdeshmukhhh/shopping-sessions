using System.Buffers;
using System.Text;

// Flat Parquet files with uncompressed PLAIN data pages. Parquet metadata and
// page headers use Thrift Compact Protocol. Only top_category has definition levels.
static class Parquet
{
    private const int Int32 = 1, Int64 = 2, ByteArray = 6;
    private const int Utf8 = 0, DateType = 6;
    private const int Plain = 0, Rle = 3;
    private const int RowsPerGroup = 32_768;

    public sealed record Column<T>(string Name, int Type, int? ConvertedType, bool Optional,
        Func<T, long>? Numeric, Func<T, ulong?>? StringKey);

    public static Column<T> Text<T>(string name, Func<T, ulong?> read, bool optional = false) =>
        new(name, ByteArray, Utf8, optional, null, read);
    public static Column<T> Integer<T>(string name, Func<T, long> read) =>
        new(name, Int64, null, false, read, null);
    public static Column<T> Date<T>(string name, Func<T, long> read) =>
        new(name, Int32, DateType, false, read, null);

    private readonly record struct Chunk(long Offset, int Length);
    private readonly record struct Group(int Rows, Chunk[] Chunks);

    public static void Write<T>(string path, IReadOnlyList<T> rows, Keys keys, params Column<T>[] columns)
    {
        using var file = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16);
        using var writer = new BinaryWriter(file);
        using var page = new MemoryStream();
        using var values = new BinaryWriter(page);
        writer.Write("PAR1"u8);
        var groups = new List<Group>();

        for (int start = 0; start < rows.Count; start += RowsPerGroup)
        {
            int count = Math.Min(RowsPerGroup, rows.Count - start);
            var chunks = new Chunk[columns.Length];
            for (int c = 0; c < columns.Length; c++)
            {
                page.SetLength(0);
                EncodePage(values, rows, start, count, columns[c], keys);
                int payload = (int)page.Length;
                long offset = file.Position;
                var header = new Compact(writer);
                header.Int(1, 0); // DATA_PAGE
                header.Int(2, payload);
                header.Int(3, payload);
                var data = header.Struct(5);
                data.Int(1, count);
                data.Int(2, Plain);
                data.Int(3, Rle);
                data.Int(4, Rle);
                data.End();
                header.End();
                writer.Write(page.GetBuffer().AsSpan(0, payload));
                chunks[c] = new Chunk(offset, (int)(file.Position - offset));
            }
            groups.Add(new Group(count, chunks));
        }

        long footerStart = file.Position;
        var metadata = new Compact(writer);
        metadata.Int(1, 1);
        metadata.List(2, Compact.StructType, columns.Length + 1);
        var root = new Compact(writer);
        root.Text(4, "schema");
        root.Int(5, columns.Length);
        root.End();
        foreach (var column in columns)
        {
            var schema = new Compact(writer);
            schema.Int(1, column.Type);
            schema.Int(3, column.Optional ? 1 : 0);
            schema.Text(4, column.Name);
            if (column.ConvertedType is int type) schema.Int(6, type);
            schema.End();
        }
        metadata.Long(3, rows.Count);
        metadata.List(4, Compact.StructType, groups.Count);
        foreach (var group in groups)
        {
            var rowGroup = new Compact(writer);
            rowGroup.List(1, Compact.StructType, columns.Length);
            long bytes = 0;
            for (int i = 0; i < columns.Length; i++)
            {
                var column = columns[i];
                var chunk = group.Chunks[i];
                bytes += chunk.Length;
                var columnChunk = new Compact(writer);
                columnChunk.Long(2, 0); // Column metadata is inline in the footer.
                var columnMeta = columnChunk.Struct(3);
                columnMeta.Int(1, column.Type);
                columnMeta.List(2, Compact.IntType, 2);
                columnMeta.Signed(Plain);
                columnMeta.Signed(Rle);
                columnMeta.List(3, Compact.BinaryType, 1);
                columnMeta.String(column.Name);
                columnMeta.Int(4, 0); // UNCOMPRESSED
                columnMeta.Long(5, group.Rows);
                columnMeta.Long(6, chunk.Length);
                columnMeta.Long(7, chunk.Length);
                columnMeta.Long(9, chunk.Offset);
                columnMeta.End();
                columnChunk.End();
            }
            rowGroup.Long(2, bytes);
            rowGroup.Long(3, group.Rows);
            rowGroup.End();
        }
        metadata.Text(6, "shopping-sessions");
        metadata.End();
        writer.Write((int)(file.Position - footerStart));
        writer.Write("PAR1"u8);
    }

    private static void EncodePage<T>(BinaryWriter writer, IReadOnlyList<T> rows, int start, int count,
        Column<T> column, Keys keys)
    {
        if (column.StringKey is { } read)
        {
            if (column.Optional)
            {
                // A single bit-packed run of definition levels, padded to a multiple of eight.
                int groups = (count + 7) / 8;
                uint run = (uint)(groups * 2 + 1);
                int headerLength = run < 128 ? 1 : run < 16_384 ? 2 : 3;
                writer.Write(groups + headerLength);
                Compact.Unsigned(writer, run);
                for (int offset = 0; offset < count; offset += 8)
                {
                    int mask = 0;
                    for (int bit = 0; bit < 8 && offset + bit < count; bit++)
                        if (read(rows[start + offset + bit]).HasValue) mask |= 1 << bit;
                    writer.Write((byte)mask);
                }
            }
            Span<byte> buffer = stackalloc byte[9];
            for (int i = start; i < start + count; i++)
            {
                if (read(rows[i]) is not ulong key) continue;
                var payload = keys.Bytes(key, buffer);
                writer.Write(payload.Length);
                writer.Write(payload);
            }
        }
        else if (column.Type == Int32)
        {
            for (int i = start; i < start + count; i++) writer.Write((int)column.Numeric!(rows[i]));
        }
        else
        {
            for (int i = start; i < start + count; i++) writer.Write(column.Numeric!(rows[i]));
        }
    }

    private sealed class Compact(BinaryWriter writer)
    {
        public const int IntType = 5, BinaryType = 8, StructType = 12;
        private int previous;

        private void Field(int id, int type)
        {
            int delta = id - previous;
            if (delta is > 0 and <= 15) writer.Write((byte)((delta << 4) | type));
            else
            {
                writer.Write((byte)type);
                Signed(id);
            }
            previous = id;
        }

        public void End() => writer.Write((byte)0);

        public void Int(int id, int value) { Field(id, IntType); Signed(value); }
        public void Long(int id, long value) { Field(id, 6); Signed(value); }
        public void Text(int id, string value) { Field(id, BinaryType); String(value); }
        public Compact Struct(int id) { Field(id, StructType); return new Compact(writer); }

        public void List(int id, int type, int count)
        {
            Field(id, 9);
            writer.Write((byte)((Math.Min(count, 15) << 4) | type));
            if (count >= 15) Unsigned(writer, (uint)count);
        }

        public void String(string value)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            Unsigned(writer, (uint)bytes.Length);
            writer.Write(bytes);
        }

        public void Signed(long value) => Unsigned(writer, (ulong)((value << 1) ^ (value >> 63)));

        public static void Unsigned(BinaryWriter writer, ulong value)
        {
            while (value >= 128)
            {
                writer.Write((byte)((value & 127) | 128));
                value >>= 7;
            }
            writer.Write((byte)value);
        }
    }
}

