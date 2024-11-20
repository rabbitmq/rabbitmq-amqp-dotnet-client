// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

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
    : IntegrationTest(testOutputHelper, setupConnectionAndManagement: false)
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
        Assert.Null(_connection);
        Assert.Null(_management);

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(_containerId).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(activeRecovery).Topology(false)).Build());

        TaskCompletionSource<bool> connectionClosedStateTcs = CreateTaskCompletionSource<bool>();
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
                connectionClosedStateTcs.SetResult(true);
            }
        };

        Assert.Equal(State.Open, connection.State);
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        await connectionClosedStateTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
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
        Assert.Null(_connection);
        Assert.Null(_management);

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(_containerId).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(true).Topology(false)
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())).Build());

        TaskCompletionSource<bool> listErrorCountGreaterThanOrEqualToTwoTcs = CreateTaskCompletionSource<bool>();
        TaskCompletionSource<bool> listErrorCountGreaterThanOrEqualToFourTcs = CreateTaskCompletionSource<bool>();

        var listFromStatus = new List<State>();
        var listToStatus = new List<State>();
        var listError = new List<Error?>();
        connection.ChangeState += (sender, previousState, currentState, error) =>
        {
            try
            {
                listFromStatus.Add(previousState);
                listToStatus.Add(currentState);
                listError.Add(error);
                if (listError.Count >= 2)
                {
                    // Note: must use try since it'll be called again
                    listErrorCountGreaterThanOrEqualToTwoTcs.TrySetResult(true);
                }

                if (listError.Count >= 4)
                {
                    listErrorCountGreaterThanOrEqualToFourTcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                listErrorCountGreaterThanOrEqualToTwoTcs.TrySetException(ex);
                listErrorCountGreaterThanOrEqualToFourTcs.SetException(ex);
            }
        };

        Assert.Equal(State.Open, connection.State);
        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);
        await listErrorCountGreaterThanOrEqualToTwoTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(State.Open, listFromStatus[0]);
        Assert.Equal(State.Reconnecting, listToStatus[0]);
        Assert.NotNull(listError[0]);

        Assert.Equal(State.Reconnecting, listFromStatus[1]);
        Assert.Equal(State.Open, listToStatus[1]);
        Assert.Null(listError[1]);

        await connection.CloseAsync();
        await listErrorCountGreaterThanOrEqualToFourTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

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
        Assert.Null(_connection);
        Assert.Null(_management);

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(_containerId).RecoveryConfiguration(
                RecoveryConfiguration.Create().Activated(true).Topology(false).BackOffDelayPolicy(
                    new FakeBackOffDelayPolicyDisabled())).Build());

        var listFromStatus = new List<State>();
        TaskCompletionSource<bool> listFromStatusCountGreaterOrEqualToTwo = CreateTaskCompletionSource<bool>();
        TaskCompletionSource<bool> listErrorCountGreaterOrEqualToTwo = CreateTaskCompletionSource<bool>();

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

            if (listFromStatus.Count >= 2)
            {
                listFromStatusCountGreaterOrEqualToTwo.TrySetResult(true);
            }

            if (listError.Count >= 2)
            {
                listErrorCountGreaterOrEqualToTwo.SetResult(true);
            }
        };

        Assert.Equal(State.Open, connection.State);
        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);

        await listFromStatusCountGreaterOrEqualToTwo.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(State.Open, listFromStatus[0]);
        Assert.Equal(State.Reconnecting, listToStatus[0]);
        Assert.NotNull(listError[0]);

        Assert.Equal(State.Reconnecting, listFromStatus[1]);
        Assert.Equal(State.Closed, listToStatus[1]);
        Assert.NotNull(listError[1]);
        Assert.Equal("CONNECTION_NOT_RECOVERED", listError[1].ErrorCode);

        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        await listErrorCountGreaterOrEqualToTwo.Task.WaitAsync(TimeSpan.FromSeconds(5));
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
        Assert.Null(_connection);
        Assert.Null(_management);

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(true))
                .ContainerId(_containerId)
                .Build());
        TaskCompletionSource<bool> twoRecoveryEventsSeenTcs = CreateTaskCompletionSource<bool>();
        int recoveryEvents = 0;
        connection.ChangeState += (sender, from, to, error) =>
        {
            if (Interlocked.Increment(ref recoveryEvents) == 2)
            {
                twoRecoveryEventsSeenTcs.SetResult(true);
            }
        };
        IManagement management = connection.Management();
        ITopologyListener topologyListener = ((IManagementTopology)management).TopologyListener();
        await management.Queue().Name(_queueName).AutoDelete(true).Exclusive(true).DeclareAsync();
        Assert.Equal(1, topologyListener.QueueCount());

        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);
        await twoRecoveryEventsSeenTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await SystemUtils.WaitUntilFuncAsync(() => recoveryEvents == 2);

        await SystemUtils.WaitUntilQueueExistsAsync(_queueName);

        await connection.CloseAsync();

        await SystemUtils.WaitUntilQueueDeletedAsync(_queueName);

        Assert.Equal(0, topologyListener.QueueCount());
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
        Assert.Null(_connection);
        Assert.Null(_management);

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(false))
                .ContainerId(_containerId)
                .Build());
        TaskCompletionSource<bool> oneRecoveryEventSeenTcs = CreateTaskCompletionSource<bool>();
        int recoveryEvents = 0;
        connection.ChangeState += (sender, from, to, error) =>
        {
            if (Interlocked.Increment(ref recoveryEvents) == 1)
            {
                oneRecoveryEventSeenTcs.SetResult(true);
            }
        };
        IManagement management = connection.Management();
        ITopologyListener topologyListener = ((IManagementTopology)management).TopologyListener();
        await management.Queue().Name(_queueName).AutoDelete(true).Exclusive(true).DeclareAsync();
        Assert.Equal(1, topologyListener.QueueCount());

        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);
        await oneRecoveryEventSeenTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await SystemUtils.WaitUntilQueueDeletedAsync(_queueName);

        await connection.CloseAsync();
        Assert.Equal(0, topologyListener.QueueCount());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RecoveryTopologyShouldRecoverExchanges(bool topologyEnabled)
    {
        Assert.Null(_connection);
        Assert.Null(_management);

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(topologyEnabled))
                .ContainerId(_containerId)
                .Build());
        TaskCompletionSource<bool> twoRecoveryEventsSeenTcs = CreateTaskCompletionSource<bool>();
        int recoveryEvents = 0;
        connection.ChangeState += (sender, from, to, error) =>
        {
            if (Interlocked.Increment(ref recoveryEvents) == 2)
            {
                twoRecoveryEventsSeenTcs.SetResult(true);
            }
        };
        IManagement management = connection.Management();
        ITopologyListener topologyListener = ((IManagementTopology)management).TopologyListener();
        IExchangeSpecification exSpec = management.Exchange().Name(_exchangeName).AutoDelete(true)
            .Type(ExchangeType.DIRECT);
        await exSpec.DeclareAsync();
        Assert.Equal(1, topologyListener.ExchangeCount());

        // Since we cannot reboot the broker for this test we delete the exchange manually to simulate check if  
        // the exchange is recovered.
        await SystemUtils.DeleteExchangeAsync(_exchangeName);

        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);

        await twoRecoveryEventsSeenTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        if (topologyEnabled)
        {
            await SystemUtils.WaitUntilExchangeExistsAsync(_exchangeName);
        }
        else
        {
            await SystemUtils.WaitUntilExchangeDeletedAsync(_exchangeName);
        }

        Assert.Equal(1, topologyListener.ExchangeCount());

        await exSpec.DeleteAsync();

        Assert.Equal(0, topologyListener.ExchangeCount());

        await connection.CloseAsync();
        connection.Dispose();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RecoveryTopologyShouldRecoverBindings(bool topologyEnabled)
    {
        Assert.Null(_connection);
        Assert.Null(_management);

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(topologyEnabled))
                .ContainerId(_containerId)
                .Build());
        TaskCompletionSource<bool> twoRecoveryEventsSeenTcs = CreateTaskCompletionSource<bool>();
        int recoveryEvents = 0;
        connection.ChangeState += (sender, from, to, error) =>
        {
            if (Interlocked.Increment(ref recoveryEvents) == 2)
            {
                twoRecoveryEventsSeenTcs.SetResult(true);
            }
        };
        IManagement management = connection.Management();
        ITopologyListener topologyListener = ((IManagementTopology)management).TopologyListener();
        IExchangeSpecification exSpec = management.Exchange().Name(_exchangeName).AutoDelete(true)
            .Type(ExchangeType.DIRECT);
        await exSpec.DeclareAsync();
        Assert.Equal(1, topologyListener.ExchangeCount());

        IQueueSpecification queueSpec = management.Queue().Name(_queueName).AutoDelete(true).Exclusive(true);
        await queueSpec.DeclareAsync();

        Assert.Equal(1, topologyListener.QueueCount());
        IBindingSpecification bindingSpec =
            management.Binding().SourceExchange(exSpec).DestinationQueue(queueSpec).Key("key");
        await bindingSpec.BindAsync();
        Assert.Equal(1, topologyListener.BindingCount());

        // Since we cannot reboot the broker for this test we delete the exchange manually to simulate check if  
        // the exchange is recovered.
        await SystemUtils.DeleteExchangeAsync(_exchangeName);

        // The queue will be deleted due of the auto-delete flag
        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);
        await twoRecoveryEventsSeenTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        if (topologyEnabled)
        {
            await SystemUtils.WaitUntilExchangeExistsAsync(_exchangeName);
            await SystemUtils.WaitUntilQueueExistsAsync(_queueName);
            await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync(_exchangeName, _queueName);
        }
        else
        {
            await SystemUtils.WaitUntilExchangeDeletedAsync(_exchangeName);
            await SystemUtils.WaitUntilQueueDeletedAsync(_queueName);
            await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(_exchangeName, _queueName);
        }

        Assert.Equal(1, topologyListener.ExchangeCount());
        Assert.Equal(1, topologyListener.QueueCount());
        Assert.Equal(1, topologyListener.BindingCount());

        await exSpec.DeleteAsync();
        await queueSpec.DeleteAsync();
        await bindingSpec.UnbindAsync();

        Assert.Equal(0, topologyListener.ExchangeCount());
        Assert.Equal(0, topologyListener.QueueCount());
        Assert.Equal(0, topologyListener.BindingCount());

        await connection.CloseAsync();
    }

    /// <summary>
    /// Test if removed a queue should remove the bindings.
    /// In this case there are two queues, one will be deleted to check if the bindings are removed.
    /// another queue will not be deleted to check if the bindings are not removed.
    ///
    /// Only at the end the queue will be removed and bindings should be zero.
    /// </summary>
    [Fact]
    public async Task RemoveAQueueShouldRemoveTheBindings()
    {
        Assert.Null(_connection);
        Assert.Null(_management);

        string wontDeleteQueueName = _queueName + "-wont-delete";

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(true))
                .ContainerId(_containerId)
                .Build());

        IManagement management = connection.Management();
        ITopologyListener topologyListener = ((IManagementTopology)management).TopologyListener();
        IExchangeSpecification exSpec = management.Exchange().Name(_exchangeName)
            .Type(ExchangeType.DIRECT);

        await exSpec.DeclareAsync();

        IQueueSpecification queueSpec = management.Queue().Name(_queueName)
            .AutoDelete(true).Exclusive(true);
        await queueSpec.DeclareAsync();

        IQueueSpecification queueSpecWontDelete = management.Queue().Name(wontDeleteQueueName)
            .AutoDelete(true).Exclusive(true);
        await queueSpecWontDelete.DeclareAsync();

        for (int i = 0; i < 10; i++)
        {
            await management.Binding().SourceExchange(exSpec)
                .DestinationQueue(queueSpec).Key($"key_{i}").BindAsync();

            await management.Binding().SourceExchange(exSpec)
                .DestinationQueue(queueSpecWontDelete).Key($"key_{i}").BindAsync();
        }

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync(_exchangeName, _queueName);

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync(_exchangeName, wontDeleteQueueName);

        Assert.Equal(20, topologyListener.BindingCount());
        await queueSpec.DeleteAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(_exchangeName, _queueName);

        Assert.Equal(10, topologyListener.BindingCount());
        await queueSpecWontDelete.DeleteAsync();

        await exSpec.DeleteAsync();

        Assert.Equal(0, topologyListener.ExchangeCount());
        Assert.Equal(0, topologyListener.QueueCount());

        await connection.CloseAsync();
    }

    [Fact]
    public async Task RemoveAnExchangeShouldRemoveTheBindings()
    {
        Assert.Null(_connection);
        Assert.Null(_management);

        string wontDeleteExchangeName = _exchangeName + "-wont-delete";

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(true))
                .ContainerId(_containerId)
                .Build());

        IManagement management = connection.Management();
        ITopologyListener topologyListener = ((IManagementTopology)management).TopologyListener();
        IExchangeSpecification exSpec = management.Exchange().Name(_exchangeName)
            .Type(ExchangeType.DIRECT);

        await exSpec.DeclareAsync();

        IExchangeSpecification exSpecWontDeleted = management.Exchange().Name(wontDeleteExchangeName)
            .Type(ExchangeType.DIRECT);

        await exSpecWontDeleted.DeclareAsync();

        IQueueSpecification queueSpec = management.Queue().Name(_queueName)
            .AutoDelete(true).Exclusive(true);
        await queueSpec.DeclareAsync();

        for (int i = 0; i < 10; i++)
        {
            await management.Binding().SourceExchange(exSpec)
                .DestinationQueue(queueSpec).Key($"key_{i}").BindAsync();

            await management.Binding().SourceExchange(exSpecWontDeleted)
                .DestinationQueue(queueSpec).Key($"key_{i}").BindAsync();
        }

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync(_exchangeName,
            _queueName);

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync(
            wontDeleteExchangeName,
            _queueName);

        Assert.Equal(20, topologyListener.BindingCount());
        await exSpec.DeleteAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(
            _exchangeName,
            _queueName);

        Assert.Equal(10, topologyListener.BindingCount());
        await exSpecWontDeleted.DeleteAsync();

        await queueSpec.DeleteAsync();

        Assert.Equal(0, topologyListener.ExchangeCount());
        Assert.Equal(0, topologyListener.QueueCount());

        await connection.CloseAsync();
    }

    /// <summary>
    /// This test is specific to check if the bindings are removed when a destination exchange is removed.
    /// </summary>
    [Fact]
    public async Task RemoveAnExchangeBoundToAnotherExchangeShouldRemoveTheBindings()
    {
        Assert.Null(_connection);
        Assert.Null(_management);

        string destinationExchangeName = _exchangeName + "-destination";

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create()
                .RecoveryConfiguration(RecoveryConfiguration.Create()
                    .BackOffDelayPolicy(new FakeFastBackOffDelay())
                    .Topology(true))
                .ContainerId(_containerId)
                .Build());

        IManagement management = connection.Management();
        ITopologyListener topologyListener = ((IManagementTopology)management).TopologyListener();

        IExchangeSpecification exSpec = management.Exchange().Name(_exchangeName)
            .Type(ExchangeType.DIRECT);

        await exSpec.DeclareAsync();

        IExchangeSpecification exSpecDestination = management.Exchange()
            .Name(destinationExchangeName)
            .Type(ExchangeType.DIRECT);

        await exSpecDestination.DeclareAsync();

        for (int i = 0; i < 10; i++)
        {
            await management.Binding().SourceExchange(exSpec)
                .DestinationExchange(exSpecDestination).Key($"key_{i}").BindAsync();
        }

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(_exchangeName, destinationExchangeName);

        Assert.Equal(10, topologyListener.BindingCount());

        await exSpecDestination.DeleteAsync();
        Assert.Equal(0, topologyListener.BindingCount());

        await exSpec.DeleteAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(_exchangeName, destinationExchangeName);

        Assert.Equal(0, topologyListener.ExchangeCount());

        await connection.CloseAsync();
    }
}
