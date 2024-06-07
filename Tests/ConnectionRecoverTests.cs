using System;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace Tests;

using Xunit;

public class ConnectionRecoverTests
{

    [Fact]
    public async void NormalCloseShouldSetUnexpectedFlagToFalse()
    {
        AmqpConnection connection = new();
        var completion = new TaskCompletionSource<bool>();
        connection.Closed += (sender, unexpected) =>
        {
            Assert.Equal(Status.Closed, connection.Status);
            completion.SetResult(unexpected);
        };

        var connectionName = Guid.NewGuid().ToString();
        await connection.ConnectAsync(new AmqpAddressBuilder().ConnectionName(connectionName).Build());
        Assert.Equal(Status.Open, connection.Status);
        await connection.CloseAsync();

        var result = await completion.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(result);
        Assert.Equal(Status.Closed, connection.Status);
    }
    [Fact]
    public async void UnexpectedCloseShouldSetUnexpectedFlag()
    {
        AmqpConnection connection = new();
        var completion = new TaskCompletionSource<bool>();
        connection.Closed += (sender, unexpected) =>
        {
            if (!unexpected) return;
            Assert.Equal(Status.Closed, connection.Status);
            completion.SetResult(true);
        };

        var connectionName = Guid.NewGuid().ToString();
        await connection.ConnectAsync(new AmqpAddressBuilder().ConnectionName(connectionName).Build());
        SystemUtils.WaitUntilConnectionIsKilled(connectionName);

        var result = await completion.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result);
        Assert.Equal(Status.Closed, connection.Status);
        await connection.CloseAsync();
        Assert.Equal(Status.Closed, connection.Status);
    }


    [Fact]
    public async void RecoverFromUnexpectedClose()
    {
        AmqpConnection connection = new();
        var completion = new TaskCompletionSource<Status>();
        connection.Closed += async (sender, unexpected) =>
        {
            if (!unexpected) return;
            Assert.Equal(Status.Closed, connection.Status);
            Assert.True(unexpected);
            SystemUtils.Wait();
            await connection.EnsureConnectionAsync();
            completion.SetResult(connection.Status);
        };

        var connectionName = Guid.NewGuid().ToString();
        await connection.ConnectAsync(new AmqpAddressBuilder().ConnectionName(connectionName).Build());
        SystemUtils.WaitUntilConnectionIsKilled(connectionName);

        var result = await completion.Task.WaitAsync(TimeSpan.FromSeconds(25));
        Assert.Equal(Status.Open, result);
        await connection.CloseAsync();
        Assert.Equal(Status.Closed, connection.Status);
    }
}