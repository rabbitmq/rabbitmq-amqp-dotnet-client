using RabbitMQ.AMQP.Client;

namespace Tests;

using Xunit;

public class ConnectionTests
{
    [Fact]
    public void ValidateAddress()
    {
        Address address = new("localhost", 5672);
        Assert.Equal("localhost", address.Host);
        Assert.Equal(5672, address.Port);
        Assert.Equal("Address{host='localhost', port=5672}", address.ToString());
        
        Address address2 = new("localhost", 5672);
        Assert.Equal(address, address2);
        
        Address address3 = new("localhost", 5673);
        Assert.NotEqual(address, address3);
        
        Address address4 = new("localhost", 5672);
        Assert.Equal(address.GetHashCode(), address4.GetHashCode());
        
        Address address5 = new("localhost", 5673);
        Assert.NotEqual(address.GetHashCode(), address5.GetHashCode());
    }

}