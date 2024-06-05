using System;
using System.Net.Sockets;
using Amqp;
using Amqp.Framing;
using RabbitMQ.AMQP.Client;

namespace Tests;

using Xunit;

public class ConnectionTests
{
    [Fact]
    public void ValidateAddress()
    {
        AmqpAddress amqpAddress = new("localhost", 5672, "guest-user",
            "guest-password", "vhost_1", "amqp1", "connection_name");
        Assert.Equal("localhost", amqpAddress.Host());
        Assert.Equal(5672, amqpAddress.Port());
        Assert.Equal("guest-user", amqpAddress.User());
        Assert.Equal("guest-password", amqpAddress.Password());
        Assert.Equal("vhost_1", amqpAddress.VirtualHost());
        Assert.Equal("amqp1", amqpAddress.Scheme());

        AmqpAddress second = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp1", "connection_name");

        Assert.Equal(amqpAddress, second);

        AmqpAddress third = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp2", "connection_name");

        Assert.NotEqual(amqpAddress, third);
    }

    [Fact]
    public void ValidateAddressBuilder()
    {
        var address = new AmqpAddressBuilder()
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
        Assert.Equal("amqp12", address.Scheme());
    }

    [Fact]
    public async void RaiseErrorsIfTheParametersAreNotValid()
    {
        AmqpConnection connection = new();
        await Assert.ThrowsAsync<ConnectionException>(async () => await connection.ConnectAsync(new
        AmqpAddressBuilder().VirtualHost("wrong_vhost").Build()));
        Assert.Equal(Status.Closed, connection.Status);


        await Assert.ThrowsAsync<SocketException>(async () => await connection.ConnectAsync(new
            AmqpAddressBuilder().Host("wrong_host").Build()));
        Assert.Equal(Status.Closed, connection.Status);


        await Assert.ThrowsAsync<ConnectionException>(async () => await connection.ConnectAsync(new
            AmqpAddressBuilder().Password("wrong_password").Build()));
        Assert.Equal(Status.Closed, connection.Status);

        await Assert.ThrowsAsync<ConnectionException>(async () => await connection.ConnectAsync(new
            AmqpAddressBuilder().User("wrong_user").Build()));
        Assert.Equal(Status.Closed, connection.Status);

        await Assert.ThrowsAsync<SocketException>(async () => await connection.ConnectAsync(new
            AmqpAddressBuilder().Port(0).Build()));
        Assert.Equal(Status.Closed, connection.Status);


        await Assert.ThrowsAsync<ConnectionException>(async () => await connection.ConnectAsync(new
            AmqpAddressBuilder().Scheme("wrong_scheme").Build()));
        Assert.Equal(Status.Closed, connection.Status);
    }

}