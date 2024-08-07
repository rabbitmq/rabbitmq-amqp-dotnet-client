using System.Security.Cryptography;
using System.Text;
using System.Web;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public enum DeliveryMode
{
    AtMostOnce,
    AtLeastOnce
}

public static class Utils
{
    private const string DefaultPrefix = "client.gen-";

    public static string GenerateQueueName()
    {
        return GenerateName(DefaultPrefix);
    }

    private static string GenerateName(string prefix)
    {
        string uuid = Guid.NewGuid().ToString();
        byte[] digest = MD5.HashData(Encoding.UTF8.GetBytes(uuid));
        return prefix + Convert.ToBase64String(digest)
            .Replace('+', '-')
            .Replace('/', '_')
            .Replace("=", "");
    }

    public static RabbitMQ.AMQP.Client.Error? ConvertError(Amqp.Framing.Error? sourceError)
    {
        Error? resultError = null;
        if (sourceError == null)
        {
            return resultError;
        }

        resultError = new Error(sourceError.Condition.ToString(), sourceError.Description);
        return resultError;
    }

    public static void ValidateNonNegative(string label, long value, long max)
    {
        if (value < 0)
        {
            throw new ArgumentException($"'{label}' must be greater than or equal to 0");
        }

        if (max > 0)
        {
            if (value > max)
            {
                throw new ArgumentException($"'{label}' must be lesser than {max}");
            }
        }
    }

    public static void ValidatePositive(string label, long value, long max)
    {
        if (value <= 0)
        {
            throw new ArgumentException($"'{label}' must be greater than or equal to 0");
        }

        if (max >= 0)
        {
            if (value > max)
            {
                throw new ArgumentException($"'{label}' must be lesser than {max}");
            }
        }
    }

    public static void ValidatePositive(string label, long value)
    {
        if (value <= 0)
        {
            throw new ArgumentException($"'{label}' must be greater than or equal to 0");
        }
    }

    // switch (options.deliveryMode()) {
    //     case AT_MOST_ONCE:
    //         protonSender.setSenderSettleMode(SenderSettleMode.SETTLED);
    //         protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
    //         break;
    //     case AT_LEAST_ONCE:
    //         protonSender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
    //         protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
    //         break;

    public static Attach CreateAttach(string address,
        DeliveryMode deliveryMode, string linkName, Map? sourceFilter = null)
    {
        var attach = new Attach
        {
            SndSettleMode = deliveryMode == DeliveryMode.AtMostOnce
                ? SenderSettleMode.Settled
                : SenderSettleMode.Unsettled,
            RcvSettleMode = ReceiverSettleMode.First,
            LinkName = linkName,

            // Role = true,
            Target = new Target()
            {
                Address = address,
                ExpiryPolicy = new Symbol("SESSION_END"),
                Dynamic = false,
                Durable = 0,
            },
            Source = new Source()
            {
                Address = address,
                ExpiryPolicy = new Symbol("LINK_DETACH"),
                Timeout = 0,
                Dynamic = false,
                Durable = 0,
                FilterSet = sourceFilter
            }
        };
        return attach;
    }

    public static string? EncodePathSegment(string url)
    {
        return PercentCodec.EncodePathSegment(url);
    }

    public static string EncodeHttpParameter(string url)
    {
        return HttpUtility.UrlPathEncode(url);
    }

    public static bool CompareMap(Map map1, Map map2)
    {
        if (map1.Count != map2.Count)
        {
            return false;
        }

        foreach (object? key in map1.Keys)
        {
            if (!map2.ContainsKey(key))
            {
                return false;
            }

            if (!map1[key].Equals(map2[key]))
            {
                return false;
            }
        }
        return true;
    }
}

public static class PercentCodec
{
    private const int Radix = 16;
    private static readonly bool[] s_unreserved;

    static PercentCodec()
    {
        s_unreserved = new bool[256];
        for (int i = 'A'; i <= 'Z'; i++)
        {
            s_unreserved[i] = true;
        }

        for (int i = 'a'; i <= 'z'; i++)
        {
            s_unreserved[i] = true;
        }

        for (int i = '0'; i <= '9'; i++)
        {
            s_unreserved[i] = true;
        }

        s_unreserved['-'] = true;
        s_unreserved['.'] = true;
        s_unreserved['_'] = true;
        s_unreserved['~'] = true;
    }

    public static string? EncodePathSegment(string? segment)
    {
        if (segment == null)
        {
            return null;
        }

        StringBuilder buf = new();
        byte[] bytes = Encoding.UTF8.GetBytes(segment);

        foreach (byte b in bytes)
        {
            if (s_unreserved[b])
            {
                buf.Append((char)b);
            }
            else
            {
                buf.Append('%');
                char hex1 = char.ToUpper(Convert.ToChar((b >> 4).ToString("X")[0]));
                char hex2 = char.ToUpper(Convert.ToChar((b & 0xF).ToString("X")[0]));
                buf.Append(hex1);
                buf.Append(hex2);
            }
        }

        return buf.ToString();
    }
}
