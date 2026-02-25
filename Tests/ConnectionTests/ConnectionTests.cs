// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.ConnectionTests;

public class ConnectionTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task RaiseErrorsIfTheParametersAreNotValid()
    {
        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingsBuilder.Create().VirtualHost("wrong_vhost").Build()));

        // TODO check inner exception is a SocketException
        await Assert.ThrowsAnyAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingsBuilder.Create().Host("wrong_host").Build()));

        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingsBuilder.Create().Password("wrong_password").Build()));

        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingsBuilder.Create().User("wrong_user").Build()));

        // TODO check inner exception is a SocketException
        await Assert.ThrowsAnyAsync<ConnectionException>(async () =>
            await AmqpConnection.CreateAsync(ConnectionSettingsBuilder.Create().Port(1234).Build()));
    }

    [Fact]
    public async Task ThrowAmqpClosedExceptionWhenItemIsClosed()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();
        await publisher.CloseAsync();
        publisher.Dispose();

        await Assert.ThrowsAsync<AmqpNotOpenException>(() =>
        {
            var message = new AmqpMessage("Hello wold!");
            return publisher.PublishAsync(message);
        });

        await queueSpec.DeleteAsync();
        await _connection.CloseAsync();
        Assert.Empty(_connection.Publishers);

        await Assert.ThrowsAsync<AmqpNotOpenException>(() =>
            _connection.PublisherBuilder().Queue("ThrowAmqpClosedExceptionWhenItemIsClosed").BuildAsync());

        await Assert.ThrowsAsync<AmqpNotOpenException>(async () =>
            await _management.Queue().Name("ThrowAmqpClosedExceptionWhenItemIsClosed").DeclareAsync());
    }

    // override the connection setting to the connection 
    [Fact]
    public async Task CreateAConnectionWithOverriddenConnectionSettings()
    {
        string connectionName = "ConnectionWithOverriddenSettings_" + Guid.NewGuid();
        IEnvironment environment = AmqpEnvironment.Create(ConnectionSettingsBuilder.Create().ContainerId(connectionName).Build());
        Assert.NotNull(environment);
        IConnection connection = await environment.ConnectionBuilder()
            .CreateConnectionAsync();
        await WaitUntilConnectionIsOpen(connectionName);
        // create another connection with different connection settings, but it should not override the first connection's container id
        string connectionName2 = "ConnectionWithOverriddenSettings2_" + Guid.NewGuid();
        IConnection connection2 = await environment.ConnectionBuilder()
            .ConnectionSettings(ConnectionSettingsBuilder.Create().ContainerId(connectionName2).Build())
            .CreateConnectionAsync();
        await WaitUntilConnectionIsOpen(connectionName2);
        Assert.NotNull(connection2);
        // now create another connection with wrong password settings, and it should raise exception, but it should not affect the existing connections
        await Assert.ThrowsAsync<ConnectionException>(async () =>
            await environment.ConnectionBuilder()
                .ConnectionSettings(ConnectionSettingsBuilder.Create().Password("wrong_password").Build())
                .CreateConnectionAsync());

        // just to be sure the existing connections are still open and working, we can check the state of the connections, and it should be open
        await WaitUntilConnectionIsOpen(connectionName);
        await WaitUntilConnectionIsOpen(connectionName2);
        await connection2.CloseAsync();
        await connection.CloseAsync();
        await environment.CloseAsync();
    }

}
