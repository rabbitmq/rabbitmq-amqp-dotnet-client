using System.Text.RegularExpressions;

namespace RabbitMQ.AMQP.Client;

public partial class ByteCapacity : IEquatable<ByteCapacity>
{
    public long Bytes { get; }
    private string _input;

    private ByteCapacity(long bytes, string input)
    {
        _input = input;
        Bytes = bytes;
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


    // private static int KILOBYTES_MULTIPLIER = 1000;

    // private static final Pattern PATTERN =
    // Pattern.compile("^(?<size>\\d*)((?<unit>kb|mb|gb|tb))?$", Pattern.CASE_INSENSITIVE);

    // private const string Pattern = @"(?i)^(?<size>\\d*)((?<unit>kb|mb|gb|tb))?$";
    private static readonly Regex SizeRegex = new Regex(@"^(\d+)([kKmMgGtTpP]?[bB]?)$", RegexOptions.Compiled);

    // public static ByteCapacity from(String value) {
    //     Matcher matcher = PATTERN.matcher(value);
    // }

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
        return Bytes;
    }


// Create a Regex
    // Regex rg = new Regex(pattern);

    // private static final String UNIT_MB = "mb";
    // private static final int KILOBYTES_MULTIPLIER = 1000;
    // private static final int MEGABYTES_MULTIPLIER = 1000 * 1000;
    // private static final int GIGABYTES_MULTIPLIER = 1000 * 1000 * 1000;
    // private static final long TERABYTES_MULTIPLIER = 1000L * 1000L * 1000L * 1000L;
    //
    // private static final Pattern PATTERN =
    //     Pattern.compile("^(?<size>\\d*)((?<unit>kb|mb|gb|tb))?$", Pattern.CASE_INSENSITIVE);
    // private static final String GROUP_SIZE = "size";
    // private static final String GROUP_UNIT = "unit";
    // private static final String UNIT_KB = "kb";
    // private static final String UNIT_GB = "gb";
    // private static final String UNIT_TB = "tb";
    //
    // private static final Map<String, BiFunction<Long, String, ByteCapacity>> CONSTRUCTORS =
    //     Collections.unmodifiableMap(
    //         new HashMap<String, BiFunction<Long, String, ByteCapacity>>() {
    //           {
    //             put(UNIT_KB, (size, input) -> ByteCapacity.kB(size, input));
    //             put(UNIT_MB, (size, input) -> ByteCapacity.MB(size, input));
    //             put(UNIT_GB, (size, input) -> ByteCapacity.GB(size, input));
    //             put(UNIT_TB, (size, input) -> ByteCapacity.TB(size, input));
    //           }
    //         });
    //
    // private final long bytes;
    // private final String input;
    //
    // private ByteCapacity(long bytes) {
    //   this(bytes, String.valueOf(bytes));
    // }
    //
    // private ByteCapacity(long bytes, String input) {
    //   this.bytes = bytes;
    //   this.input = input;
    // }
    //
    // public static ByteCapacity B(long bytes) {
    //   return new ByteCapacity(bytes);
    // }
    //
    // public static ByteCapacity kB(long kilobytes) {
    //   return new ByteCapacity(kilobytes * KILOBYTES_MULTIPLIER);
    // }
    //
    // public static ByteCapacity MB(long megabytes) {
    //   return new ByteCapacity(megabytes * MEGABYTES_MULTIPLIER);
    // }
    //
    // public static ByteCapacity GB(long gigabytes) {
    //   return new ByteCapacity(gigabytes * GIGABYTES_MULTIPLIER);
    // }
    //
    // public static ByteCapacity TB(long terabytes) {
    //   return new ByteCapacity(terabytes * TERABYTES_MULTIPLIER);
    // }
    //
    // private static ByteCapacity kB(long kilobytes, String input) {
    //   return new ByteCapacity(kilobytes * KILOBYTES_MULTIPLIER, input);
    // }
    //
    // private static ByteCapacity MB(long megabytes, String input) {
    //   return new ByteCapacity(megabytes * MEGABYTES_MULTIPLIER, input);
    // }
    //
    // private static ByteCapacity GB(long gigabytes, String input) {
    //   return new ByteCapacity(gigabytes * GIGABYTES_MULTIPLIER, input);
    // }
    //
    // private static ByteCapacity TB(long terabytes, String input) {
    //   return new ByteCapacity(terabytes * TERABYTES_MULTIPLIER, input);
    // }
    //
    // public long toBytes() {
    //   return bytes;
    // }
    //
    // public static ByteCapacity from(String value) {
    //   Matcher matcher = PATTERN.matcher(value);
    //   if (matcher.matches()) {
    //     long size = Long.valueOf(matcher.group(GROUP_SIZE));
    //     String unit = matcher.group(GROUP_UNIT);
    //     ByteCapacity result;
    //     if (unit == null) {
    //       result = ByteCapacity.B(size);
    //     } else {
    //       return CONSTRUCTORS
    //           .getOrDefault(
    //               unit.toLowerCase(),
    //               (v, input) -> {
    //                 throw new IllegalArgumentException("Unknown capacity unit: " + unit);
    //               })
    //           .apply(size, value);
    //     }
    //     return result;
    //   } else {
    //     throw new IllegalArgumentException("Cannot parse value for byte capacity: " + value);
    //   }


    // }


    public bool Equals(ByteCapacity? other)
    {
        throw new NotImplementedException();
    }
}