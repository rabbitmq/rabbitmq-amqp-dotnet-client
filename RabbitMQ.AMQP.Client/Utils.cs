// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
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
        [ThreadStatic] private static Random? s_random;
#endif

        internal static int RandomNext(int minValue = 0, int maxValue = 1024)
        {
#if NET6_0_OR_GREATER
            return s_random.Next(minValue, maxValue);
#else
            s_random ??= new Random();
            return s_random.Next(minValue, maxValue);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static TaskCompletionSource<T> CreateTaskCompletionSource<T>()
        {
            return new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
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

        internal static Attach CreateAttach(string? address,
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

        internal static bool IsValidScheme(string scheme)
        {
            if (scheme.Equals("amqp", StringComparison.InvariantCultureIgnoreCase) ||
                scheme.Equals("amqps", StringComparison.InvariantCultureIgnoreCase))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        internal static void ValidateMessageAnnotations(Dictionary<string, object> annotations)
        {
            foreach (KeyValuePair<string, object> kvp in annotations)
            {
                ValidateMessageAnnotationKey(kvp.Key);
            }
        }

        internal static void ValidateMessageAnnotationKey(string key)
        {
            if (false == key.StartsWith("x-"))
            {
                throw new ArgumentOutOfRangeException($"Message annotation key must start with 'x-': {key}");
            }
        }

        /// <summary>
        /// This function is used to wait until a condition is met.
        /// with a backoff strategy. Useful for waiting on a condition during a reconnection for example
        /// </summary>
        /// <param name="func">The function to test</param>
        /// <param name="callBack">Call back to check what's happening</param>
        /// <param name="retries">Number of retries</param>
        internal static async Task WaitWithBackOffUntilFuncAsync(
            Func<Task<bool>> func, Action<bool, TimeSpan> callBack, ushort retries = 10)
        {
            TimeSpan backOffTimeSpan = TimeSpan.FromSeconds(5);

            while (false == await func().ConfigureAwait(false))
            {
                callBack(false, backOffTimeSpan);
                await Task.Delay(backOffTimeSpan).ConfigureAwait(false);
                backOffTimeSpan = TimeSpan.FromSeconds(backOffTimeSpan.TotalSeconds * 2);

                --retries;
                if (retries == 0)
                {
                    throw new TimeoutException("Timeout waiting for condition to be met");
                }
            }
            callBack(true, backOffTimeSpan);
        }

        internal static bool SupportsFilterExpressions(string brokerVersion)
        {
            return Is4_1_OrMore(brokerVersion);
        }

        internal static bool Is4_0_OrMore(string brokerVersion)
        {
            return VersionCompare(CurrentVersion(brokerVersion), "4.0.0") >= 0;
        }

        internal static bool Is4_1_OrMore(string brokerVersion)
        {
            return VersionCompare(CurrentVersion(brokerVersion), "4.1.0") >= 0;
        }

        internal static bool Is4_2_OrMore(string brokerVersion)
        {
            return VersionCompare(CurrentVersion(brokerVersion), "4.2.0") >= 0;
        }

        private static string CurrentVersion(string currentVersion)
        {
            // versions built from source: 3.7.0+rc.1.4.gedc5d96
            if (currentVersion.Contains("+"))
            {
                currentVersion = currentVersion.Substring(0, currentVersion.IndexOf("+"));
            }
            // alpha (snapshot) versions: 3.7.0~alpha.449-1
            if (currentVersion.Contains("~"))
            {
                currentVersion = currentVersion.Substring(0, currentVersion.IndexOf("~"));
            }
            // alpha (snapshot) versions: 3.7.1-alpha.40
            if (currentVersion.Contains("-"))
            {
                currentVersion = currentVersion.Substring(0, currentVersion.IndexOf("-"));
            }
            return currentVersion;
        }

        /// <summary>
        /// https://stackoverflow.com/questions/6701948/efficient-way-to-compare-version-strings-in-java
        /// </summary>
        private static int VersionCompare(string str1, string str2)
        {
            string[] vals1 = str1.Split('.');
            string[] vals2 = str2.Split('.');
            int i = 0;

            // set index to first non-equal ordinal or length of shortest version string
            while (i < vals1.Length && i < vals2.Length && vals1[i].Equals(vals2[i]))
            {
                i++;
            }

            // compare first non-equal ordinal number
            if (i < vals1.Length && i < vals2.Length)
            {
                int val1 = int.Parse(vals1[i]);
                int val2 = int.Parse(vals2[i]);
                return val1.CompareTo(val2);
            }

            // the strings are equal or one string is a substring of the other
            // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
            return vals1.Length.CompareTo(vals2.Length);
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
