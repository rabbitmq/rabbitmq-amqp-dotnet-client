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
    [InlineData("1TB", 1000L * 1000L * 1000L * 1000L)]
    public void FromShouldReturnTheCorrectBytesValues(string input, long expectedBytes)
    {
        Assert.Equal(expectedBytes, ByteCapacity.From(input).ToBytes());
    }

    [Fact]
    public void FromShouldThrowExceptionWhenInvalidInput()
    {
        Assert.Throws<ArgumentException>(() => ByteCapacity.From("invalid"));
    }
    
    // [Fact]
    // public void ()
    // {
    //     Assert.Throws<ArgumentException>(() => ByteCapacity.From("1"));
    // }

}