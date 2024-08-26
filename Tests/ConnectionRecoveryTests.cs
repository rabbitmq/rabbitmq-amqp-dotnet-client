using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit.Abstractions;

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
    public int CurrentAttempt => 1;
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
    public int CurrentAttempt => 1;
}

public class ConnectionRecoveryTests(ITestOutputHelper testOutputHelper)
{
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;

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
        _testOutputHelper.WriteLine($"NormalCloseTheStatusShouldBeCorrectAndErrorNull: {activeRecovery}");
        string containerId = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(containerId).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(activeRecovery).Topology(false)).Build());

        TaskCompletionSource completion = new TaskCompletionSource();
        var listFromStatus = new List<State>();
        var listToStatus = new List<State>();
        var listError = new List<Error?>();
        connection.ChangeState += (sender, from, to, error) =>
        {
            listFromStatus.Add(from);
            listToStatus.Add(to);
            listError.Add(error);
            if (to == State.Closed)
            {
                completion.SetResult();
            }
        };

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
        
        _testOutputHelper.WriteLine("UnexpectedCloseTheStatusShouldBeCorrectAndErrorNotNull");
        const string containerId = "unexpected-close-connection-name";
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(containerId).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(true).Topology(false)
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())).Build());
        var resetEvent = new ManualResetEvent(false);
        var listFromStatus = new List<State>();
        var listToStatus = new List<State>();
        var listError = new List<Error?>();
        connection.ChangeState += (sender, previousState, currentState, error) =>
        {
            listFromStatus.Add(previousState);
            listToStatus.Add(currentState);
            listError.Add(error);
            if (listError.Count >= 4)
            {
                resetEvent.Set();
            }
        };

        Assert.Equal(State.Open, connection.State);
        await SystemUtils.WaitUntilConnectionIsKilled(containerId);
        resetEvent.WaitOne(TimeSpan.FromSeconds(5));
        await SystemUtils.WaitUntilFuncAsync(() => (listFromStatus.Count >= 2));
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
        await SystemUtils.WaitUntilFuncAsync(() => (listFromStatus.Count >= 4));

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
        _testOutputHelper.WriteLine("OverrideTheBackOffWithBackOffDisabled");
        string containerId = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(containerId).RecoveryConfiguration(
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
            if (error is not null)
            {
                listError.Add(error);
            }

            if (listError.Count >= 4)
            {
                resetEvent.Set();
            }
        };

        Assert.Equal(State.Open, connection.State);
        await SystemUtils.WaitUntilConnectionIsKilled(containerId);
        resetEvent.WaitOne(TimeSpan.FromSeconds(5));
        await SystemUtils.WaitUntilFuncAsync(() => (listFromStatus.Count >= 2));
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
        _testOutputHelper.WriteLine("RecoveryTopologyShouldRecoverTheTempQueues");
        string queueName = $"temp-queue-should-recover-{true}";
        const string containerId = "temp-queue-should-recover-connection-name";
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(true))
                .ContainerId(containerId)
                .Build());
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int recoveryEvents = 0;
        connection.ChangeState += (sender, from, to, error) =>
        {
            if (Interlocked.Increment(ref recoveryEvents) == 2)
            {
                completion.SetResult(true);
            }
        };
        var management = connection.Management();
        await management.Queue().Name(queueName).AutoDelete(true).Exclusive(true).DeclareAsync();
        Assert.Equal(1, management.TopologyListener().QueueCount());


        await SystemUtils.WaitUntilConnectionIsKilled(containerId);
        await completion.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await SystemUtils.WaitUntilFuncAsync(() => recoveryEvents == 2);

        await SystemUtils.WaitUntilQueueExistsAsync(queueName);

        await connection.CloseAsync();

        await SystemUtils.WaitUntilQueueDeletedAsync(queueName);

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
        _testOutputHelper.WriteLine("RecoveryTopologyShouldNotRecoverTheTempQueues");
        string queueName = $"temp-queue-should-recover-{false}";
        const string containerId = "temp-queue-should-not-recover-connection-name";
        var connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(false))
                .ContainerId(containerId)
                .Build());
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int recoveryEvents = 0;
        connection.ChangeState += (sender, from, to, error) =>
        {
            if (Interlocked.Increment(ref recoveryEvents) == 1)
            {
                completion.SetResult(true);
            }
        };
        var management = connection.Management();
        await management.Queue().Name(queueName).AutoDelete(true).Exclusive(true).DeclareAsync();
        Assert.Equal(1, management.TopologyListener().QueueCount());


        await SystemUtils.WaitUntilConnectionIsKilled(containerId);
        await completion.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await SystemUtils.WaitUntilQueueDeletedAsync(queueName);

        await connection.CloseAsync();
        Assert.Equal(0, management.TopologyListener().QueueCount());
    }


    // [Theory]
    // [InlineData(true)]
    // [InlineData(false)]
    // public async Task RecoveryTopologyShouldRecoverExchanges(bool topologyEnabled)
    // {
    //     const string containerId = "exchange-should-recover-connection-name";
    //     var connection = await AmqpConnection.CreateAsync(
    //         ConnectionSettingBuilder.Create()
    //             .RecoveryConfiguration(RecoveryConfiguration.Create()
    //                 .BackOffDelayPolicy(new FakeFastBackOffDelay())
    //                 .Topology(topologyEnabled))
    //             .ContainerId(containerId)
    //             .Build());
    //     TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    //     int recoveryEvents = 0;
    //     connection.ChangeState += (sender, from, to, error) =>
    //     {
    //         if (Interlocked.Increment(ref recoveryEvents) == 2)
    //         {
    //             completion.SetResult(true);
    //         }
    //     };
    //     var management = connection.Management();
    //     var exSpec = management.Exchange().Name("exchange-should-recover").AutoDelete(true)
    //         .Type(ExchangeType.DIRECT);
    //     await exSpec.DeclareAsync();
    //     Assert.Equal(1, management.TopologyListener().ExchangeCount());
    //
    //     // Since we cannot reboot the broker for this test we delete the exchange manually to simulate check if  
    //     // the exchange is recovered.
    //     await SystemUtils.DeleteExchangeAsync("exchange-should-recover");
    //     await SystemUtils.WaitUntilConnectionIsKilled(containerId);
    //     await completion.Task.WaitAsync(TimeSpan.FromSeconds(10));
    //
    //     if (topologyEnabled)
    //     {
    //         await SystemUtils.WaitUntilExchangeExistsAsync("exchange-should-recover");
    //     }
    //     else
    //     {
    //         await SystemUtils.WaitUntilExchangeDeletedAsync("exchange-should-recover");
    //     }
    //
    //     Assert.Equal(1, management.TopologyListener().ExchangeCount());
    //
    //     await exSpec.DeleteAsync();
    //
    //     Assert.Equal(0, management.TopologyListener().ExchangeCount());
    //
    //     await connection.CloseAsync();
    // }
    //
    //
    // [Theory]
    // [InlineData(true)]
    // [InlineData(false)]
    // public async Task RecoveryTopologyShouldRecoverBindings(bool topologyEnabled)
    // {
    //     const string containerId = "binding-should-recover-connection-name";
    //     var connection = await AmqpConnection.CreateAsync(
    //         ConnectionSettingBuilder.Create()
    //             .RecoveryConfiguration(RecoveryConfiguration.Create()
    //                 .BackOffDelayPolicy(new FakeFastBackOffDelay())
    //                 .Topology(topologyEnabled))
    //             .ContainerId(containerId)
    //             .Build());
    //     TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    //     int recoveryEvents = 0;
    //     connection.ChangeState += (sender, from, to, error) =>
    //     {
    //         if (Interlocked.Increment(ref recoveryEvents) == 2)
    //         {
    //             completion.SetResult(true);
    //         }
    //     };
    //     var management = connection.Management();
    //     var exSpec = management.Exchange().Name("exchange-should-recover-binding").AutoDelete(true)
    //         .Type(ExchangeType.DIRECT);
    //     await exSpec.DeclareAsync();
    //     Assert.Equal(1, management.TopologyListener().ExchangeCount());
    //     var queueSpec = management.Queue().Name("queue-should-recover-binding").AutoDelete(true).Exclusive(true);
    //     await queueSpec.DeclareAsync();
    //     Assert.Equal(1, management.TopologyListener().QueueCount());
    //     var bindingSpec =
    //         management.Binding().SourceExchange(exSpec).DestinationQueue(queueSpec).Key("key");
    //     await bindingSpec.BindAsync();
    //     Assert.Equal(1, management.TopologyListener().BindingCount());
    //
    //     // Since we cannot reboot the broker for this test we delete the exchange manually to simulate check if  
    //     // the exchange is recovered.
    //     await SystemUtils.DeleteExchangeAsync("exchange-should-recover-binding");
    //
    //     // The queue will be deleted due of the auto-delete flag
    //     await SystemUtils.WaitUntilConnectionIsKilled(containerId);
    //     await completion.Task.WaitAsync(TimeSpan.FromSeconds(10));
    //
    //     if (topologyEnabled)
    //     {
    //         await SystemUtils.WaitUntilExchangeExistsAsync("exchange-should-recover-binding");
    //         await SystemUtils.WaitUntilQueueExistsAsync("queue-should-recover-binding");
    //         await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync("exchange-should-recover-binding",
    //             "queue-should-recover-binding");
    //     }
    //     else
    //     {
    //         await SystemUtils.WaitUntilExchangeDeletedAsync("exchange-should-recover-binding");
    //         await SystemUtils.WaitUntilQueueDeletedAsync("queue-should-recover-binding");
    //         await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync("exchange-should-recover-binding",
    //             "queue-should-recover-binding");
    //     }
    //
    //     Assert.Equal(1, management.TopologyListener().ExchangeCount());
    //     Assert.Equal(1, management.TopologyListener().QueueCount());
    //     Assert.Equal(1, management.TopologyListener().BindingCount());
    //
    //     await exSpec.DeleteAsync();
    //     await queueSpec.DeleteAsync();
    //     await bindingSpec.UnbindAsync();
    //
    //     Assert.Equal(0, management.TopologyListener().ExchangeCount());
    //     Assert.Equal(0, management.TopologyListener().QueueCount());
    //     Assert.Equal(0, management.TopologyListener().BindingCount());
    //
    //     await connection.CloseAsync();
    // }
}
