using System;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests;

public class AmqpEnvironmentTests
{
    [Fact]
    public async Task CreateAConnectionWithEnvironment()
    {
        IEnvironment env = await AmqpEnvironment.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IConnection connection = await env.CreateConnectionAsync();
        Assert.NotNull(connection);
        Assert.NotEmpty(env.GetConnections());
        await env.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        Assert.Empty(env.GetConnections());
    }

    [Fact]
    public async Task CreateMoreConnectionsWithDifferentParametersEnvironment()
    {
        string envConnectionName = "EnvironmentConnection_" + Guid.NewGuid().ToString();
        IEnvironment env = await AmqpEnvironment.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(envConnectionName).Build());

        IConnection connection = await env.CreateConnectionAsync();
        Assert.NotNull(connection);
        await SystemUtils.WaitUntilAsync(async () => await SystemUtils.IsConnectionOpen(envConnectionName));
        Assert.NotEmpty(env.GetConnections());
        Assert.Single(env.GetConnections());

        string envConnectionName2 = "EnvironmentConnection2_" + Guid.NewGuid().ToString();

        IConnection connection2 = await env.CreateConnectionAsync(
            ConnectionSettingBuilder.Create().ConnectionName(envConnectionName2).Build());
        Assert.NotNull(connection2);
        Assert.Equal(2, env.GetConnections().Count);
        await SystemUtils.WaitUntilAsync(async () => await SystemUtils.IsConnectionOpen(envConnectionName2));

        await env.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        Assert.Equal(State.Closed, connection2.State);
        Assert.Empty(env.GetConnections());
    }

    [Fact]
    public async Task CloseConnectionsIndividuals()
    {
        string envConnectionName = "EnvironmentConnection_" + Guid.NewGuid().ToString();
        IEnvironment env = await AmqpEnvironment.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(envConnectionName).Build());
        IConnection connection = await env.CreateConnectionAsync();
        await SystemUtils.WaitUntilAsync(async () => await SystemUtils.IsConnectionOpen(envConnectionName));


        string envConnectionName2 = "EnvironmentConnection2_" + Guid.NewGuid().ToString();
        IConnection connection2 = await env.CreateConnectionAsync(
            ConnectionSettingBuilder.Create().ConnectionName(envConnectionName2).Build());
        Assert.Equal(2, env.GetConnections().Count);
        await SystemUtils.WaitUntilAsync(async () => await SystemUtils.IsConnectionOpen(envConnectionName2));


        string envConnectionName3 = "EnvironmentConnection3_" + Guid.NewGuid().ToString();
        IConnection connection3 = await env.CreateConnectionAsync(
            ConnectionSettingBuilder.Create().ConnectionName(envConnectionName3).Build());
        Assert.Equal(3, env.GetConnections().Count);
        await SystemUtils.WaitUntilAsync(async () => await SystemUtils.IsConnectionOpen(envConnectionName3));


        // closing 
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        Assert.Equal(2, env.GetConnections().Count);

        await SystemUtils.WaitUntilAsync(async () => await SystemUtils.IsConnectionOpen(envConnectionName) == false);
        await connection2.CloseAsync();
        Assert.Equal(State.Closed, connection2.State);
        Assert.Single(env.GetConnections());
        await SystemUtils.WaitUntilAsync(async () => await SystemUtils.IsConnectionOpen(envConnectionName2) == false);


        await connection3.CloseAsync();
        Assert.Equal(State.Closed, connection3.State);
        await SystemUtils.WaitUntilAsync(async () => await SystemUtils.IsConnectionOpen(envConnectionName3) == false);


        Assert.Empty(env.GetConnections());
        await env.CloseAsync();
    }
}
