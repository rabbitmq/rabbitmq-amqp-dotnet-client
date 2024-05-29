using RabbitMQ.AMQP.Client;

namespace Tests;

using Xunit;

public class ConnectionTests
{
    [Fact]
    public void ValidateAddress()
    {
        AmqpAddress amqpAddress = new("localhost", 5672);
        Assert.Equal("localhost", amqpAddress.Host);
        Assert.Equal(5672, amqpAddress.Port);
        Assert.Equal("Address{host='localhost', port=5672}", amqpAddress.ToString());

        AmqpAddress address2 = new("localhost", 5672);
        Assert.Equal(amqpAddress, address2);

        AmqpAddress address3 = new("localhost", 5673);
        Assert.NotEqual(amqpAddress, address3);

        AmqpAddress address4 = new("localhost", 5672);
        Assert.Equal(amqpAddress.GetHashCode(), address4.GetHashCode());

        AmqpAddress address5 = new("localhost", 5673);
        Assert.NotEqual(amqpAddress.GetHashCode(), address5.GetHashCode());
    }

}