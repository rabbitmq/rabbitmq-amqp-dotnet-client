// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class EnvironmentTests(ITestOutputHelper testOutputHelper)
    : IntegrationTest(testOutputHelper, setupConnectionAndManagement: false)
{
    [Fact]
    public async Task CreateAConnectionWithEnvironment()
    {
        IEnvironment env = AmqpEnvironment.Create(ConnectionSettingsBuilder.Create().Build());
        var amqpEnv = (AmqpEnvironment)env;

        IConnection connection = await env.CreateConnectionAsync();

        Assert.NotNull(connection);
        Assert.NotEmpty(amqpEnv.Connections);
        await env.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        Assert.Empty(amqpEnv.Connections);
    }

    [Fact]
    public async Task CreateMoreConnectionsWithDifferentParametersEnvironment()
    {
        string envConnectionName = "EnvironmentConnection_" + Guid.NewGuid();

        IEnvironment env = AmqpEnvironment.Create(
            ConnectionSettingsBuilder.Create().ContainerId(envConnectionName).Build());
        var amqpEnv = (AmqpEnvironment)env;

        IConnection connection = await env.CreateConnectionAsync();

        Assert.NotNull(connection);
        await WaitUntilConnectionIsOpen(envConnectionName);
        Assert.NotEmpty(amqpEnv.Connections);
        Assert.Single(amqpEnv.Connections);

        string envConnectionName2 = "EnvironmentConnection2_" + Guid.NewGuid();

        IConnection connection2 = await env.CreateConnectionAsync(
            ConnectionSettingsBuilder.Create().ContainerId(envConnectionName2).Build());
        Assert.NotNull(connection2);
        Assert.Equal(2, amqpEnv.Connections.Count);
        await WaitUntilConnectionIsOpen(envConnectionName2);

        await env.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        Assert.Equal(State.Closed, connection2.State);
        Assert.Empty(amqpEnv.Connections);
    }

    [Fact]
    public async Task CloseConnectionsIndividually()
    {
        string envConnectionName = "EnvironmentConnection_" + Guid.NewGuid();

        IEnvironment env = AmqpEnvironment.Create(
            ConnectionSettingsBuilder.Create().ContainerId(envConnectionName).Build());
        var amqpEnv = (AmqpEnvironment)env;

        IConnection connection = await env.CreateConnectionAsync();

        await WaitUntilConnectionIsOpen(envConnectionName);
        Assert.Single(amqpEnv.Connections);
        Assert.Equal(1, amqpEnv.Connections[0].Id);

        string envConnectionName2 = "EnvironmentConnection2_" + Guid.NewGuid().ToString();
        IConnection connection2 = await env.CreateConnectionAsync(
            ConnectionSettingsBuilder.Create().ContainerId(envConnectionName2).Build());
        Assert.Equal(2, amqpEnv.Connections.Count);
        Assert.Equal(2, amqpEnv.Connections[1].Id);
        await WaitUntilConnectionIsOpen(envConnectionName2);

        string envConnectionName3 = "EnvironmentConnection3_" + Guid.NewGuid().ToString();
        IConnection connection3 = await env.CreateConnectionAsync(
            ConnectionSettingsBuilder.Create().ContainerId(envConnectionName3).Build());
        Assert.Equal(3, amqpEnv.Connections.Count);
        Assert.Equal(3, amqpEnv.Connections[2].Id);
        await WaitUntilConnectionIsOpen(envConnectionName3);

        // closing 
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        Assert.Equal(2, amqpEnv.Connections.Count);

        await WaitUntilConnectionIsClosed(envConnectionName);
        await connection2.CloseAsync();
        Assert.Equal(State.Closed, connection2.State);
        Assert.Single(amqpEnv.Connections);
        await WaitUntilConnectionIsClosed(envConnectionName2);

        await connection3.CloseAsync();
        Assert.Equal(State.Closed, connection3.State);
        await WaitUntilConnectionIsClosed(envConnectionName3);

        Assert.Empty(amqpEnv.Connections);
        await env.CloseAsync();
    }
}
