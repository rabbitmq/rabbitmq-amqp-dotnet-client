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
    
    
    /// <summary>
    /// The normal close the status should be correct and error null
    /// The test records the status change when the connection is closed normally.
    /// The error _must_ be null when the connection is closed normally even the recovery is activated. 
    /// </summary>
    /// <param name="activeRecovery"> If the recovery is enabled.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async void NormalCloseTheStatusShouldBeCorrectAndErrorNull(bool activeRecovery)
    {
        var connectionName = Guid.NewGuid().ToString();
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(activeRecovery).Topology(false)).Build());

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
        Assert.Equal(Status.Open, listFromStatus[0]);
        Assert.Equal(Status.Closed, listToStatus[0]);
        Assert.Null(listError[0]);
    }

    
    /// <summary>
    /// The unexpected close the status should be correct and error not null.
    /// The connection is closed unexpectedly using HTTP API.
    /// The test validates the different status changes:
    ///  - From Open to Reconnecting (With error)
    ///  - From Reconnecting to Open
    ///
    /// then the connection is closed normally. so the status should be:
    ///  - From Open to Closed
    /// </summary>
    [Fact]
    public async void UnexpectedCloseTheStatusShouldBeCorrectAndErrorNotNull()
    {
        var connectionName = Guid.NewGuid().ToString();
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(true).Topology(false)).Build());
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
        SystemUtils.WaitUntil(() => (listFromStatus.Count >= 2));
        Assert.Equal(Status.Open, listFromStatus[0]);
        Assert.Equal(Status.Reconneting, listToStatus[0]);
        Assert.NotNull(listError[0]);
        Assert.Equal(Status.Reconneting, listFromStatus[1]);
        Assert.Equal(Status.Open, listToStatus[1]);
        Assert.Null(listError[1]);
        resetEvent.Reset();
        resetEvent.Set();
        await connection.CloseAsync();
        resetEvent.WaitOne(TimeSpan.FromSeconds(5));
        Assert.Equal(Status.Open, listFromStatus[2]);
        Assert.Equal(Status.Closed, listToStatus[2]);
        Assert.Null(listError[2]);
    }
}