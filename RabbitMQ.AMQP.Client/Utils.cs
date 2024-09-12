// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Security.Cryptography;
using System.Text;
using System.Web;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client
{
    internal enum DeliveryMode
    {
        AtMostOnce,
        AtLeastOnce
    }

    internal static class Utils
    {
        private const string DefaultPrefix = "client.gen-";

#if NET6_0_OR_GREATER
        private static readonly Random s_random = Random.Shared;
#else
        [ThreadStatic]
        private static Random? s_random;
#endif

        internal static int RandomNext(int minValue, int maxValue)
        {
#if NET6_0_OR_GREATER
            return s_random.Next(minValue, maxValue);
#else
            s_random ??= new Random();
            return s_random.Next(minValue, maxValue);
#endif
        }

        internal static string GenerateQueueName()
        {
            return GenerateName(DefaultPrefix);
        }

        internal static string GenerateName(string prefix)
        {
            string uuidStr = Guid.NewGuid().ToString();
            byte[] uuidBytes = Encoding.ASCII.GetBytes(uuidStr);
            var md5 = MD5.Create();
            byte[] digest = md5.ComputeHash(uuidBytes);
            return prefix + Convert.ToBase64String(digest)
                .Replace('+', '-')
                .Replace('/', '_')
                .Replace("=", "");
        }

        internal static Error? ConvertError(Amqp.Framing.Error? sourceError)
        {
            Error? resultError = null;

            if (sourceError != null)
            {
                resultError = new Error(sourceError.Condition.ToString(), sourceError.Description);
            }

            return resultError;
        }

        internal static void ValidateNonNegative(string label, long value, long max)
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

        internal static void ValidatePositive(string label, long value, long max)
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

        internal static void ValidatePositive(string label, long value)
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

        internal static Attach CreateAttach(string address,
            DeliveryMode deliveryMode, Guid linkId, Map? sourceFilter = null)
        {
            var attach = new Attach
            {
                SndSettleMode = deliveryMode == DeliveryMode.AtMostOnce
                    ? SenderSettleMode.Settled
                    : SenderSettleMode.Unsettled,
                RcvSettleMode = ReceiverSettleMode.First,
                LinkName = linkId.ToString(),
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

        internal static string? EncodePathSegment(string url)
        {
            return PercentCodec.EncodePathSegment(url);
        }

        internal static string EncodeHttpParameter(string url)
        {
            return HttpUtility.UrlEncode(url);
        }

        internal static bool CompareMap(Map map1, Map map2)
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

    // TODO why can't we use normal HTTP encoding?
    internal static class PercentCodec
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

        internal static string? EncodePathSegment(string? segment)
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
}
