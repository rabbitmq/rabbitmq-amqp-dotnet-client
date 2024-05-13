using System.Text.RegularExpressions;

namespace RabbitMQ.AMQP.Client;

public partial class ByteCapacity : IEquatable<ByteCapacity>
{
    private long _bytes;
    private string _input;

    private ByteCapacity(long bytes, string input)
    {
        _input = input;
        _bytes = bytes;
    }

    private ByteCapacity(long bytes) : this(bytes, bytes.ToString())
    {
    }

    private const int KilobytesMultiplier = 1000;
    private const int MegabytesMultiplier = 1000 * 1000;
    private const int GigabytesMultiplier = 1000 * 1000 * 1000;
    private const long TerabytesMultiplier = 1000L * 1000L * 1000L * 1000L;


    public static ByteCapacity B(long bytes)
    {
        return new ByteCapacity(bytes);
    }

    public static ByteCapacity Kb(long megabytes)
    {
        return new ByteCapacity(megabytes * KilobytesMultiplier);
    }

    public static ByteCapacity Mb(long megabytes)
    {
        return new ByteCapacity(megabytes * MegabytesMultiplier);
    }

    public static ByteCapacity Gb(long gigabytes)
    {
        return new ByteCapacity(gigabytes * GigabytesMultiplier);
    }

    public static ByteCapacity Tb(long terabytes)
    {
        return new ByteCapacity(terabytes * TerabytesMultiplier);
    }


    private static readonly Regex SizeRegex = new Regex(@"^(\d+)([kKmMgGtTpP]?[bB]?)$", RegexOptions.Compiled);

    public static ByteCapacity From(string value)
    {
        var match = SizeRegex.Match(value);
        if (!match.Success)
        {
            throw new ArgumentException("Invalid capacity size format.", nameof(value));
        }

        var size = long.Parse(match.Groups[1].Value);
        var unit = match.Groups[2].Value.ToLower();

        return unit switch
        {
            "kb" => Kb(size),
            "mb" => Mb(size),
            "gb" => Gb(size),
            "tb" => Tb(size),
            _ => B(size)
        };
    }

    public long ToBytes()
    {
        return _bytes;
    }


    public bool Equals(ByteCapacity? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return _bytes == other._bytes;
    }
}