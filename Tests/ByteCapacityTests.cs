using System;
using RabbitMQ.AMQP.Client;
using Xunit;

namespace Tests;

public class ByteCapacityTests
{
    [Theory]
    [InlineData("1b", 1)]
    [InlineData("1B", 1)]
    [InlineData("2kb", 2000)]
    [InlineData("1KB", 1000)]
    [InlineData("1mb", 1000 * 1000)]
    [InlineData("32MB", 32 * 1000 * 1000)]
    [InlineData("1gb", 1000 * 1000 * 1000)]
    [InlineData("28GB", 28 * 1000L * 1000L * 1000L)]
    [InlineData("1tb", 1000L * 1000L * 1000L * 1000L)]
    [InlineData("23TB", 23 * 1000L * 1000L * 1000L * 1000L)]
    public void FromShouldReturnTheCorrectBytesValues(string input, long expectedBytes)
    {
        Assert.Equal(expectedBytes, ByteCapacity.From(input).ToBytes());
    }

    [Fact]
    public void FromShouldThrowExceptionWhenInvalidInput()
    {
        Assert.Throws<ArgumentException>(() => ByteCapacity.From("invalid"));
    }

    [Fact]
    public void ByteCapacityShouldReturnTheValidBytes()
    {
        Assert.Equal(99, ByteCapacity.B(99).ToBytes());
        Assert.Equal(76000, ByteCapacity.Kb(76).ToBytes());
        Assert.Equal(789 * 1000 * 1000, ByteCapacity.Mb(789).ToBytes());
        Assert.Equal(134 * 1000L * 1000L * 1000L, ByteCapacity.Gb(134).ToBytes());
        Assert.Equal(12 * 1000L * 1000L * 1000L * 1000L, ByteCapacity.Tb(12).ToBytes());
    }

    [Theory]
    [InlineData(9, 9 * 1000)]
    [InlineData(76, 76 * 1000)]
    public void KbByteCapacityShouldBeEqualToB(long value, long expectedBytes)
    {
        Assert.Equal(ByteCapacity.Kb(value), ByteCapacity.B(expectedBytes));
    }

    [Theory]
    [InlineData(9, 9 * 1000L * 1000L * 1000L)]
    [InlineData(76, 76 * 1000L * 1000L * 1000L)]
    public void GbByteCapacityShouldBeEqualToB(long value, long expectedBytes)
    {
        Assert.Equal(ByteCapacity.Gb(value), ByteCapacity.B(expectedBytes));
    }
}