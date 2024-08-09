using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class ConnectionTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    [Fact]
    public void ValidateAddress()
    {
        ConnectionSettings connectionSettings = new("amqp1", "localhost", 5672, "guest-user",
            "guest-password", "vhost_1", "connection_name", SaslMechanism.External);
        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5672, connectionSettings.Port);
        Assert.Equal("guest-user", connectionSettings.User);
        Assert.Equal("guest-password", connectionSettings.Password);
        Assert.Equal("vhost_1", connectionSettings.VirtualHost);
        Assert.Equal("amqp1", connectionSettings.Scheme);
        Assert.Equal(SaslMechanism.External, connectionSettings.SaslMechanism);

        ConnectionSettings second = new("amqp1", "localhost", 5672, "guest-user",
            "guest-password", "path/", "connection_name", SaslMechanism.External);

        Assert.Equal(connectionSettings, second);

        ConnectionSettings third = new("amqp2", "localhost", 5672, "guest-user",
            "guest-password", "path/", "connection_name", SaslMechanism.Plain);

        Assert.NotEqual(connectionSettings, third);
    }

    [Fact]
    public void ValidateAddressBuilder()
    {
        ConnectionSettings connectionSettings = ConnectionSettingBuilder.Create()
            .Host("localhost")
            .VirtualHost("v1")
            .User("guest-t")
            .Password("guest-w")
            .Scheme("AMQP")
            .Build();

        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5672, connectionSettings.Port);
        Assert.Equal("guest-t", connectionSettings.User);
        Assert.Equal("guest-w", connectionSettings.Password);
        Assert.Equal("v1", connectionSettings.VirtualHost);
        Assert.Equal("AMQP", connectionSettings.Scheme);
    }

    [Fact]
    public void ValidateBuilderWithSslOptions()
    {
        ConnectionSettings connectionSettings = ConnectionSettingBuilder.Create()
            .Host("localhost")
            .VirtualHost("v1")
            .User("guest-t")
            .Password("guest-w")
            .Scheme("amqps")
            .Build();

        Assert.True(connectionSettings.UseSsl);
        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
        Assert.Equal("guest-t", connectionSettings.User);
        Assert.Equal("guest-w", connectionSettings.Password);
        Assert.Equal("v1", connectionSettings.VirtualHost);
        Assert.Equal("amqps", connectionSettings.Scheme);
    }

    [Fact]
    public async Task RaiseErrorsIfTheParametersAreNotValid()
    {
        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().VirtualHost("wrong_vhost").Build()));

        // TODO check inner exception is a SocketException
        await Assert.ThrowsAnyAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Host("wrong_host").Build()));

        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Password("wrong_password").Build()));

        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().User("wrong_user").Build()));

        // TODO check inner exception is a SocketException
        await Assert.ThrowsAnyAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Port(1234).Build()));
    }

    [Fact]
    public async Task ThrowAmqpClosedExceptionWhenItemIsClosed()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Queue().Name("ThrowAmqpClosedExceptionWhenItemIsClosed").Declare();

        IPublisher publisher = await connection.PublisherBuilder().Queue("ThrowAmqpClosedExceptionWhenItemIsClosed").BuildAsync();
        await publisher.CloseAsync();

        await Assert.ThrowsAsync<AmqpNotOpenException>(() =>
        {
            var message = new AmqpMessage("Hello wold!");
            return publisher.PublishAsync(message);
        });
        await management.QueueDeletion().Delete("ThrowAmqpClosedExceptionWhenItemIsClosed");
        await connection.CloseAsync();
        Assert.Empty(connection.GetPublishers());

        await Assert.ThrowsAsync<AmqpNotOpenException>(() =>
            connection.PublisherBuilder().Queue("ThrowAmqpClosedExceptionWhenItemIsClosed").BuildAsync());

        await Assert.ThrowsAsync<AmqpNotOpenException>(async () =>
            await management.Queue().Name("ThrowAmqpClosedExceptionWhenItemIsClosed").Declare());
    }
}
