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

    public static string EncodePathSegment(string url)
    {
        return Uri.UnescapeDataString(url);
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




