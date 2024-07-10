using System.Security.Cryptography;
using System.Text;
using System.Web;
using Amqp;
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
        var uuid = Guid.NewGuid().ToString();
        var digest = MD5.HashData(Encoding.UTF8.GetBytes(uuid));
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

    // switch (options.deliveryMode()) {
    //     case AT_MOST_ONCE:
    //         protonSender.setSenderSettleMode(SenderSettleMode.SETTLED);
    //         protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
    //         break;
    //     case AT_LEAST_ONCE:
    //         protonSender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
    //         protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
    //         break;
    public static Attach CreateSenderAttach(string address,
        DeliveryMode deliveryMode, string linkName)
    {
        var senderAttach = new Attach
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
            }
        };
        return senderAttach;
    }
    
    // from Apache HttpComponents PercentCodec
// static String encodePathSegment(String segment) {
//     if (segment == null) {
//         return null;
//     }
//     StringBuilder buf = new StringBuilder();
//     final CharBuffer cb = CharBuffer.wrap(segment);
//     final ByteBuffer bb = StandardCharsets.UTF_8.encode(cb);
//     while (bb.hasRemaining()) {
//         final int b = bb.get() & 0xff;
//         if (UNRESERVED.get(b)) {
//             buf.append((char) b);
//         } else {
//             buf.append("%");
//             final char hex1 = Character.toUpperCase(Character.forDigit((b >> 4) & 0xF, RADIX));
//             final char hex2 = Character.toUpperCase(Character.forDigit(b & 0xF, RADIX));
//             buf.append(hex1);
//             buf.append(hex2);
//         }
//     }
//     return buf.toString();
// }

    public static string EncodePathSegment(string path)
    {
        return HttpUtility.UrlEncode(path);
    }
    
}




