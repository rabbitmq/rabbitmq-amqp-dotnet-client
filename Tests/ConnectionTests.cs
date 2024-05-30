using RabbitMQ.AMQP.Client;

namespace Tests;

using Xunit;

public class ConnectionTests
{
    [Fact]
    public void ValidateAddress()
    {
        AmqpAddress amqpAddress = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp1");
        Assert.Equal("localhost", amqpAddress.Host());
        Assert.Equal(5672, amqpAddress.Port());
        Assert.Equal("guest-user", amqpAddress.User());
        Assert.Equal("guest-password", amqpAddress.Password());
        Assert.Equal("path/", amqpAddress.Path());
        Assert.Equal("amqp1", amqpAddress.Scheme());

        AmqpAddress second = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp1");

        Assert.Equal(amqpAddress, second);

        AmqpAddress third = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp2");

        Assert.NotEqual(amqpAddress, third);
    }

    [Fact]
    public void ValidateAddressBuilder()
    {
        AmqpAddress address = new AmqpAddressBuilder()
            .Host("localhost")
            .Port(5672)
            .Path("path/")
            .User("guest-t")
            .Password("guest-w")
            .Scheme("amqp1")
            .Build();

        Assert.Equal("localhost", address.Host());
        Assert.Equal(5672, address.Port());
        Assert.Equal("guest-t", address.User());
        Assert.Equal("guest-w", address.Password());
        Assert.Equal("path/", address.Path());
        Assert.Equal("amqp1", address.Scheme());
    }
}