using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Tests;

using Xunit;

internal class FakeBackOffDelayPolicyDisabled : IBackOffDelayPolicy
{
    public int Delay()
    {
        return 1;
    }

    public void Reset()
    {
    }

    public bool IsActive() => false;
}

internal class FakeFastBackOffDelay : IBackOffDelayPolicy
{
    public int Delay()
    {
        return 200;
    }

    public void Reset()
    {
    }

    public bool IsActive() => true;
}

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
    public async Task NormalCloseTheStatusShouldBeCorrectAndErrorNull(bool activeRecovery)
    {
        var connectionName = Guid.NewGuid().ToString();
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(activeRecovery).Topology(false)).Build());

        var completion = new TaskCompletionSource();
        var listFromStatus = new List<State>();
        var listToStatus = new List<State>();
        var listError = new List<Error>();
        connection.ChangeState += (sender, from, to, error) =>
        {
            listFromStatus.Add(from);
            listToStatus.Add(to);
            listError.Add(error);
            if (to == State.Closed)
                completion.SetResult();
        };

        await connection.ConnectAsync();
        Assert.Equal(State.Open, connection.State);
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        await completion.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(State.Open, listFromStatus[0]);
        Assert.Equal(State.Closing, listToStatus[0]);
        Assert.Null(listError[0]);

        Assert.Equal(State.Closing, listFromStatus[1]);
        Assert.Equal(State.Closed, listToStatus[1]);
        Assert.Null(listError[1]);
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
    public async Task UnexpectedCloseTheStatusShouldBeCorrectAndErrorNotNull()
    {
        const string connectionName = "unexpected-close-connection-name";
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(true).Topology(false)
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())).Build());
        var resetEvent = new ManualResetEvent(false);
        var listFromStatus = new List<State>();
        var listToStatus = new List<State>();
        var listError = new List<Error>();
        connection.ChangeState += (sender, previousState, currentState, error) =>
        {
            listFromStatus.Add(previousState);
            listToStatus.Add(currentState);
            listError.Add(error);
            if (listError.Count >= 4)
                resetEvent.Set();
        };

        Assert.Equal(State.Open, connection.State);
        await SystemUtils.WaitUntilConnectionIsKilled(connectionName);
        resetEvent.WaitOne(TimeSpan.FromSeconds(5));
        SystemUtils.WaitUntil(() => (listFromStatus.Count >= 2));
        Assert.Equal(State.Open, listFromStatus[0]);
        Assert.Equal(State.Reconnecting, listToStatus[0]);
        Assert.NotNull(listError[0]);
        Assert.Equal(State.Reconnecting, listFromStatus[1]);
        Assert.Equal(State.Open, listToStatus[1]);
        Assert.Null(listError[1]);
        resetEvent.Reset();
        resetEvent.Set();
        await connection.CloseAsync();
        resetEvent.WaitOne(TimeSpan.FromSeconds(5));
        Assert.Equal(State.Open, listFromStatus[2]);
        Assert.Equal(State.Closing, listToStatus[2]);
        Assert.Null(listError[2]);
        Assert.Equal(State.Closing, listFromStatus[3]);
        Assert.Equal(State.Closed, listToStatus[3]);
        Assert.Null(listError[3]);
    }


    /// <summary>
    /// Test when the connection is closed unexpectedly and the recovery is enabled
    /// but the backoff is not active.
    /// The backoff can be disabled by the user due of some condition.
    /// By default, the backoff is active it will be disabled after some failed attempts.
    /// See the BackOffDelayPolicy class for more details.
    /// </summary>
    [Fact]
    public async Task OverrideTheBackOffWithBackOffDisabled()
    {
        var connectionName = Guid.NewGuid().ToString();
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(true).Topology(false).BackOffDelayPolicy(
                    new FakeBackOffDelayPolicyDisabled())).Build());
        var resetEvent = new ManualResetEvent(false);
        var listFromStatus = new List<State>();
        var listToStatus = new List<State>();
        var listError = new List<Error>();
        connection.ChangeState += (sender, previousState, currentState, error) =>
        {
            listFromStatus.Add(previousState);
            listToStatus.Add(currentState);
            listError.Add(error);
            if (listError.Count >= 4)
                resetEvent.Set();
        };

        await connection.ConnectAsync();
        Assert.Equal(State.Open, connection.State);
        await SystemUtils.WaitUntilConnectionIsKilled(connectionName);
        resetEvent.WaitOne(TimeSpan.FromSeconds(5));
        SystemUtils.WaitUntil(() => (listFromStatus.Count >= 2));
        Assert.Equal(State.Open, listFromStatus[0]);
        Assert.Equal(State.Reconnecting, listToStatus[0]);
        Assert.NotNull(listError[0]);
        Assert.Equal(State.Reconnecting, listFromStatus[1]);
        Assert.Equal(State.Closed, listToStatus[1]);
        Assert.NotNull(listError[1]);
        Assert.Equal("CONNECTION_NOT_RECOVERED", listError[1].ErrorCode);
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
    }


    /// <summary>
    /// Test when the connection is closed unexpectedly and the recovery is enabled and the topology-recover  can be:
    /// - Enabled
    ///
    /// When the topology-recover is Enabled the temp queues should be recovered.
    /// the Queue is a temp queue with the Auto-Delete and Exclusive flag set to true. 
    /// </summary>
    [Fact]
    public async Task RecoveryTopologyShouldRecoverTheTempQueues()
    {
        var queueName = $"temp-queue-should-recover-{true}";
        const string connectionName = "temp-queue-should-recover-connection-name";
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(true))
                .ConnectionName(connectionName)
                .Build());
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var recoveryEvents = 0;
        connection.ChangeState += (sender, from, to, error) =>
        {
            recoveryEvents++;
            if (recoveryEvents == 2)
                completion.SetResult(true);
        };
        var management = connection.Management();
        await management.Queue().Name(queueName).AutoDelete(true).Exclusive(true).Declare();
        Assert.Equal(1, management.TopologyListener().QueueCount());


        await SystemUtils.WaitUntilConnectionIsKilled(connectionName);
        await completion.Task.WaitAsync(TimeSpan.FromSeconds(10));
        SystemUtils.WaitUntil(() => SystemUtils.QueueExists(queueName));

        await connection.CloseAsync();
        Assert.Equal(0, management.TopologyListener().QueueCount());
    }


    /// <summary>
    /// Test when the connection is closed unexpectedly and the recovery is enabled and the topology-recover  can be:
    /// - Disabled
    ///
    /// When the topology-recover is  Disabled the temp queues should not be recovered.
    /// the Queue is a temp queue with the Auto-Delete and Exclusive flag set to true. 
    /// </summary>
    [Fact]
    public async Task RecoveryTopologyShouldNotRecoverTheTempQueues()
    {
        var queueName = $"temp-queue-should-recover-{false}";
        const string connectionName = "temp-queue-should-not-recover-connection-name";
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(false))
                .ConnectionName(connectionName)
                .Build());
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var recoveryEvents = 0;
        connection.ChangeState += (sender, from, to, error) =>
        {
            recoveryEvents++;
            if (recoveryEvents == 1)
                completion.SetResult(true);
        };
        var management = connection.Management();
        await management.Queue().Name(queueName).AutoDelete(true).Exclusive(true).Declare();
        Assert.Equal(1, management.TopologyListener().QueueCount());


        await SystemUtils.WaitUntilConnectionIsKilled(connectionName);
        await completion.Task.WaitAsync(TimeSpan.FromSeconds(10));
        SystemUtils.WaitUntil(() => SystemUtils.QueueExists(queueName) == false);

        await connection.CloseAsync();
        Assert.Equal(0, management.TopologyListener().QueueCount());
    }
}