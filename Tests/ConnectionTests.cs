using System;
using System.Net.Sockets;
using Amqp;
using Amqp.Framing;
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
        var address = new ConnectionSettingBuilder()
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
        AmqpConnection connection = new(new
            ConnectionSettingBuilder().VirtualHost("wrong_vhost").Build());
        await Assert.ThrowsAsync<ConnectionException>(async () => await connection.ConnectAsync());
        Assert.Equal(Status.Closed, connection.Status);


        connection = new AmqpConnection(new
            ConnectionSettingBuilder().Host("wrong_host").Build());
        await Assert.ThrowsAsync<SocketException>(async () => await connection.ConnectAsync());
        Assert.Equal(Status.Closed, connection.Status);


        connection = new AmqpConnection(new
            ConnectionSettingBuilder().Password("wrong_password").Build());
        await Assert.ThrowsAsync<ConnectionException>(async () => await connection.ConnectAsync());
        Assert.Equal(Status.Closed, connection.Status);

        
        connection = new AmqpConnection(new
            ConnectionSettingBuilder().User("wrong_user").Build());
        await Assert.ThrowsAsync<ConnectionException>(async () => await connection.ConnectAsync());
        Assert.Equal(Status.Closed, connection.Status);

        
        connection = new AmqpConnection(new
            ConnectionSettingBuilder().Port(1234).Build());
        await Assert.ThrowsAsync<SocketException>(async () => await connection.ConnectAsync());
        Assert.Equal(Status.Closed, connection.Status);


        connection = new AmqpConnection(new
            ConnectionSettingBuilder().Scheme("wrong_scheme").Build());
        await Assert.ThrowsAsync<ConnectionException>(async () => await connection.ConnectAsync());
        Assert.Equal(Status.Closed, connection.Status);
    }
}