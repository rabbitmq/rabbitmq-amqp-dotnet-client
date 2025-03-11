// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Text.RegularExpressions;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// Class for specifying a binary size, with units
    /// </summary>
    public class ByteCapacity : IEquatable<ByteCapacity>
    {
        private const int KilobytesMultiplier = 1000;
        private const int MegabytesMultiplier = 1000 * 1000;
        private const int GigabytesMultiplier = 1000 * 1000 * 1000;
        private const long TerabytesMultiplier = 1000L * 1000L * 1000L * 1000L;

        private static readonly Regex s_sizeRegex = new(@"^(\d+)([kKmMgGtTpP]?[bB]?)$", RegexOptions.Compiled);

        private readonly long _bytes;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bytes"></param>
        public ByteCapacity(long bytes)
        {
            _bytes = bytes;
        }

        /// <summary>
        /// Specify an amount in bytes
        /// </summary>
        /// <param name="bytes">The size, in bytes</param>
        /// <returns><see cref="ByteCapacity"/></returns>
        public static ByteCapacity B(long bytes)
        {
            return new ByteCapacity(bytes);
        }

        /// <summary>
        /// Specify an amount, in kilobytes
        /// </summary>
        /// <param name="kilobytes">The size, in kilobytes</param>
        /// <returns><see cref="ByteCapacity"/></returns>
        public static ByteCapacity Kb(long kilobytes)
        {
            return new ByteCapacity(kilobytes * KilobytesMultiplier);
        }

        /// <summary>
        /// Specify an amount, in megabytes
        /// </summary>
        /// <param name="megabytes">The size, in megabytes</param>
        /// <returns><see cref="ByteCapacity"/></returns>
        public static ByteCapacity Mb(long megabytes)
        {
            return new ByteCapacity(megabytes * MegabytesMultiplier);
        }

        /// <summary>
        /// Specify an amount, in gigabytes
        /// </summary>
        /// <param name="gigabytes">The size, in gigabytes</param>
        /// <returns><see cref="ByteCapacity"/></returns>
        public static ByteCapacity Gb(long gigabytes)
        {
            return new ByteCapacity(gigabytes * GigabytesMultiplier);
        }

        /// <summary>
        /// Specify an amount, in terabytes
        /// </summary>
        /// <param name="terabytes">The size, in terabytes</param>
        /// <returns><see cref="ByteCapacity"/></returns>
        public static ByteCapacity Tb(long terabytes)
        {
            return new ByteCapacity(terabytes * TerabytesMultiplier);
        }

        /// <summary>
        /// Explicitly convert a string into a <see cref="ByteCapacity"/>
        /// </summary>
        /// <param name="value">The value, as string</param>
        /// <returns><see cref="ByteCapacity"/></returns>
        public static explicit operator ByteCapacity(string value)
        {
            return Parse(value);
        }

        /// <summary>
        /// Cast a <see cref="ByteCapacity"/> into a <see cref="long"/>
        /// </summary>
        /// <param name="value">The value</param>
        /// <returns>The total number of bytes.</returns>
        public static implicit operator long(ByteCapacity value)
        {
            return value._bytes;
        }

        /// <summary>
        /// Parse a string into a <see cref="ByteCapacity"/>
        /// </summary>
        /// <param name="value">The value, as string</param>
        /// <returns><see cref="ByteCapacity"/></returns>
        public static ByteCapacity Parse(string value)
        {
            Match match = s_sizeRegex.Match(value);
            if (!match.Success)
            {
                throw new ArgumentException("Invalid capacity size format.", nameof(value));
            }

            long size = long.Parse(match.Groups[1].Value);
            string unit = match.Groups[2].Value.ToLowerInvariant();

            return unit switch
            {
                "kb" => Kb(size),
                "mb" => Mb(size),
                "gb" => Gb(size),
                "tb" => Tb(size),
                _ => B(size)
            };
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified <see cref="ByteCapacity"/> value.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>true if obj is an instance of <see cref="ByteCapacity"/>and equals the value of this instance; otherwise, false.</returns>
        public override bool Equals(object? obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return Equals(obj as ByteCapacity);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified <see cref="ByteCapacity"/> value.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>true if obj has the same value as this instance; otherwise, false.</returns>
        public bool Equals(ByteCapacity? obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return _bytes == obj._bytes;
        }

        /// <summary>
        ///  Returns the hash code for this instance.
        /// </summary>
        /// <returns>A 32-bit signed integer hash code.</returns>
        public override int GetHashCode()
        {
            return _bytes.GetHashCode();
        }
    }
}
