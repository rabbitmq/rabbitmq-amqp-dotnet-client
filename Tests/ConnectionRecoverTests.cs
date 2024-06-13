using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace Tests;

using Xunit;

public class ConnectionRecoverTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async void NormalCloseTheStatusShouldBeCorrectAndErrorNull(bool activeRecovery)
    {
        var connectionName = Guid.NewGuid().ToString();
        AmqpConnection connection = new(
            new ConnectionSettingBuilder().ConnectionName(connectionName).RecoveryConfiguration(
                new RecoveryConfiguration().Activated(activeRecovery).Topology(false)).Build());
        var completion = new TaskCompletionSource();
        var listFromStatus = new List<Status>();
        var listToStatus = new List<Status>();
        var listError = new List<Error>();
        connection.ChangeStatus += (sender, from, to, error) =>
        {
            listFromStatus.Add(from);
            listToStatus.Add(to);
            listError.Add(error);
            if (to == Status.Closed)
                completion.SetResult();
        };

        await connection.ConnectAsync();
        Assert.Equal(Status.Open, connection.Status);
        await connection.CloseAsync();
        Assert.Equal(Status.Closed, connection.Status);
        await completion.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(Status.Closed, listFromStatus[0]);
        Assert.Equal(Status.Open, listToStatus[0]);
        Assert.Null(listError[0]);
        Assert.Equal(Status.Open, listFromStatus[1]);
        Assert.Equal(Status.Closed, listToStatus[1]);
        Assert.Null(listError[1]);
    }

    [Fact]
    public async void UnexpectedCloseTheStatusShouldBeCorrectAndErrorNotNull()
    {
        var connectionName = Guid.NewGuid().ToString();
        AmqpConnection connection = new(
            new ConnectionSettingBuilder().ConnectionName(connectionName).RecoveryConfiguration(
                new RecoveryConfiguration().Activated(true).Topology(false)).Build());
        var resetEvent = new ManualResetEvent(false);
        var listFromStatus = new List<Status>();
        var listToStatus = new List<Status>();
        var listError = new List<Error>();
        connection.ChangeStatus += (sender, from, to, error) =>
        {
            listFromStatus.Add(from);
            listToStatus.Add(to);
            listError.Add(error);
            if (listError.Count >= 3)
                resetEvent.Set();
        };

        await connection.ConnectAsync();
        Assert.Equal(Status.Open, connection.Status);
        SystemUtils.WaitUntilConnectionIsKilled(connectionName);
        resetEvent.WaitOne(TimeSpan.FromSeconds(5));
        Assert.Equal(Status.Closed, listFromStatus[0]);
        Assert.Equal(Status.Open, listToStatus[0]);
        Assert.Null(listError[0]);
        Assert.Equal(Status.Open, listFromStatus[1]);
        Assert.Equal(Status.Reconneting, listToStatus[1]);
        Assert.NotNull(listError[1]);
        Assert.Equal(Status.Reconneting, listFromStatus[2]);
        Assert.Equal(Status.Open, listToStatus[2]);
        Assert.Null(listError[2]);
        resetEvent.Reset();
        await connection.CloseAsync();
        resetEvent.WaitOne(TimeSpan.FromSeconds(5));
        Assert.Equal(Status.Open, listFromStatus[3]);
        Assert.Equal(Status.Closed, listToStatus[3]);
        Assert.Null(listError[3]);
    }
}