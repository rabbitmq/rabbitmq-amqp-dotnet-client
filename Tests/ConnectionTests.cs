using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using EasyNetQ.Management.Client;
using EasyNetQ.Management.Client.Model;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class ConnectionTests
{
    private readonly ITestOutputHelper _output;

    public ConnectionTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void ValidateAddress()
    {
        ConnectionSettings connectionSettings = new("localhost", 5672, "guest-user",
            "guest-password", "vhost_1", "amqp1", "connection_name", SaslMechanism.External);
        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5672, connectionSettings.Port);
        Assert.Equal("guest-user", connectionSettings.User);
        Assert.Equal("guest-password", connectionSettings.Password);
        Assert.Equal("vhost_1", connectionSettings.VirtualHost);
        Assert.Equal("amqp1", connectionSettings.Scheme);
        Assert.Equal(SaslMechanism.External, connectionSettings.SaslMechanism);

        ConnectionSettings second = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp1", "connection_name", SaslMechanism.External);

        Assert.Equal(connectionSettings, second);

        ConnectionSettings third = new("localhost", 5672, "guest-user",
            "guest-password", "path/", "amqp2", "connection_name", SaslMechanism.Plain);

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
    public async Task ConnectUsingTlsAndUserPassword()
    {
        ConnectionSettings connectionSettings = ConnectionSettingBuilder.Create()
            .Host("localhost")
            .Scheme("amqps")
            .Build();

        Assert.True(connectionSettings.UseSsl);
        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
        Assert.Equal("guest", connectionSettings.User);
        Assert.Equal("guest", connectionSettings.Password);
        Assert.Equal("/", connectionSettings.VirtualHost);
        Assert.Equal("amqps", connectionSettings.Scheme);

        IConnection connection = await AmqpConnection.CreateAsync(connectionSettings);
        Assert.Equal(State.Open, connection.State);
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
    }

    [Fact]
    public async Task ConnectUsingTlsAndClientCertificate()
    {
        string cwd = Directory.GetCurrentDirectory();

        string clientCertFile = Path.GetFullPath(Path.Join(cwd, "../../../../.ci/certs/client_localhost.p12"));
        if (false == File.Exists(clientCertFile))
        {
            clientCertFile = Path.GetFullPath(Path.Join(cwd, "../../../../../.ci/certs/client_localhost.p12"));
        }
        Assert.True(File.Exists(clientCertFile));

        var cert = new X509Certificate2(clientCertFile, "grapefruit");
        string userName = cert.Subject.Trim().Replace(" ", string.Empty);

        var managementUri = new Uri("http://localhost:15672");
        using var managementClient = new ManagementClient(managementUri, "guest", "guest");
        var userInfo = new UserInfo(null, null, []);
        await managementClient.CreateUserAsync(userName, userInfo);

        var permissionInfo = new PermissionInfo();
        await managementClient.CreatePermissionAsync("/", userName, permissionInfo);

        ConnectionSettings connectionSettings = ConnectionSettingBuilder.Create()
            .Host("localhost")
            .Scheme("amqps")
            .SaslMechanism(SaslMechanism.External)
            .Build();

        Assert.NotNull(connectionSettings.TlsSettings);
        connectionSettings.TlsSettings.ClientCertificates.Add(cert);

        Assert.True(connectionSettings.UseSsl);
        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
        Assert.Null(connectionSettings.User);
        Assert.Null(connectionSettings.Password);
        Assert.Equal("/", connectionSettings.VirtualHost);
        Assert.Equal("amqps", connectionSettings.Scheme);
        Assert.Equal(SaslMechanism.External, connectionSettings.SaslMechanism);

        IConnection connection = await AmqpConnection.CreateAsync(connectionSettings);
        Assert.Equal(State.Open, connection.State);
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
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
        IPublisher publisher = connection.PublisherBuilder().Queue("ThrowAmqpClosedExceptionWhenItemIsClosed").Build();
        await publisher.CloseAsync();
        await Assert.ThrowsAsync<AmqpClosedException>(async () =>
            await publisher.Publish(new AmqpMessage("Hello wold!"), (message, descriptor) =>
            {
                // it doest matter
            }));
        await management.QueueDeletion().Delete("ThrowAmqpClosedExceptionWhenItemIsClosed");
        await connection.CloseAsync();
        Assert.Empty(connection.GetPublishers());

        Assert.Throws<AmqpClosedException>(() =>
            connection.PublisherBuilder().Queue("ThrowAmqpClosedExceptionWhenItemIsClosed").Build());

        await Assert.ThrowsAsync<AmqpClosedException>(async () =>
            await management.Queue().Name("ThrowAmqpClosedExceptionWhenItemIsClosed").Declare());
    }
}
