using System.Net.Sockets;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace Tests;

using Xunit;

public class ConnectionTests
{
    [Fact]
    public void ValidateAddress()
    {
        ConnectionSettings connectionSettings = new("localhost", 5672, "guest-user",
            "guest-password", "vhost_1", "amqp1", "connection_name");
        Assert.Equal("localhost", connectionSettings.Host());
        Assert.Equal(5672, connectionSettings.Port());
        Assert.Equal("guest-user", connectionSettings.User());
        Assert.Equal("guest-password", connectionSettings.Password());
        Assert.Equal("vhost_1", connectionSettings.VirtualHost());
        Assert.Equal("amqp1", connectionSettings.Scheme());

        ConnectionSettings second = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp1", "connection_name");

        Assert.Equal(connectionSettings, second);

        ConnectionSettings third = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp2", "connection_name");

        Assert.NotEqual(connectionSettings, third);
    }

    [Fact]
    public void ValidateAddressBuilder()
    {
        var address = ConnectionSettingBuilder.Create()
            .Host("localhost")
            .Port(5672)
            .VirtualHost("v1")
            .User("guest-t")
            .Password("guest-w")
            .Scheme("amqp1")
            .Build();

        Assert.Equal("localhost", address.Host());
        Assert.Equal(5672, address.Port());
        Assert.Equal("guest-t", address.User());
        Assert.Equal("guest-w", address.Password());
        Assert.Equal("v1", address.VirtualHost());
        Assert.Equal("amqp1", address.Scheme());
    }

    [Fact]
    public async void RaiseErrorsIfTheParametersAreNotValid()
    {
        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().VirtualHost("wrong_vhost").Build()));
      
        await Assert.ThrowsAsync<SocketException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Host("wrong_host").Build()));
        
        
        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Password("wrong_password").Build()));


        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().User("wrong_user").Build()));

        await Assert.ThrowsAsync<SocketException>(async () =>
            await AmqpConnection.CreateAsync( ConnectionSettingBuilder.Create().Port(1234).Build()));
        
    }
}