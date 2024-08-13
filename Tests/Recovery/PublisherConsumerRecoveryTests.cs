// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Recovery;

public class PublisherConsumerRecoveryTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    /// <summary>
    /// Test the Simple case where the producer is closed and the status is changed
    /// </summary>
    [Fact]
    public async Task ProducerShouldChangeStatusWhenClosed()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();

        List<(State, State)> states = [];
        publisher.ChangeState += (sender, fromState, toState, e) =>
        {
            states.Add((fromState, toState));
        };

        Assert.Equal(State.Open, publisher.State);

        await publisher.CloseAsync();

        Assert.Equal(State.Closed, publisher.State);

        await queueSpec.DeleteAsync();
        await _connection.CloseAsync();

        Assert.Contains((State.Open, State.Closing), states);
        Assert.Contains((State.Closing, State.Closed), states);
    }


    /// <summary>
    /// Test the Simple case where the consumer is closed and the status is changed
    /// </summary>
    [Fact]
    public async Task ConsumerShouldChangeStatusWhenClosed()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        IQueueInfo queueInfo = await queueSpec.DeclareAsync();

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueInfo.Name())
            .MessageHandler((context, message) =>
            {
                return Task.CompletedTask;
            }).BuildAsync();

        List<(State, State)> states = [];
        consumer.ChangeState += (sender, fromState, toState, e) =>
        {
            states.Add((fromState, toState));
        };

        Assert.Equal(State.Open, consumer.State);

        await consumer.CloseAsync();

        Assert.Equal(State.Closed, consumer.State);

        await queueSpec.DeleteAsync();
        await _connection.CloseAsync();

        Assert.Contains((State.Open, State.Closing), states);
        Assert.Contains((State.Closing, State.Closed), states);
    }

    /// <summary>
    /// Test the case where the _connection is killed and the producer status is changed
    /// In this case the `states` list should contain the following states:
    /// - Open -> Reconnecting
    /// - Reconnecting -> Open
    /// - Open -> Closing
    /// - Closing -> Closed
    /// </summary>
    [Fact]
    public async Task ProducerShouldChangeStatusWhenConnectionIsKilled()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder()
            .Queue(queueSpec)
            .BuildAsync();

        List<(State, State)> states = [];
        publisher.ChangeState += (sender, fromState, toState, e) =>
        {
            states.Add((fromState, toState));
        };

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(_containerId);

        await SystemUtils.WaitUntilFuncAsync(() => publisher.State == State.Open);

        Assert.Equal(State.Open, publisher.State);
        await publisher.CloseAsync();
        Assert.Equal(State.Closed, publisher.State);

        await queueSpec.DeleteAsync();
        await _connection.CloseAsync();

        Assert.Contains((State.Open, State.Reconnecting), states);
        Assert.Contains((State.Reconnecting, State.Open), states);
        Assert.Contains((State.Open, State.Closing), states);
        Assert.Contains((State.Closing, State.Closed), states);
    }

    /// <summary>
    /// Test the case where the _connection is killed and the consumer status is changed
    /// In this case the `states` list should contain the following states:
    /// - Open -> Reconnecting
    /// - Reconnecting -> Open
    /// - Open -> Closing
    /// - Closing -> Closed
    /// </summary>
    [Fact]
    public async Task ConsumerShouldChangeStatusWhenConnectionIsKilled()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .MessageHandler((context, message) =>
            {
                return Task.CompletedTask;
            }).BuildAsync();

        List<(State, State)> states = [];
        consumer.ChangeState += (sender, fromState, toState, e) => { states.Add((fromState, toState)); };

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(_containerId);

        await SystemUtils.WaitUntilFuncAsync(() => consumer.State == State.Open);

        Assert.Equal(State.Open, consumer.State);
        await consumer.CloseAsync();
        Assert.Equal(State.Closed, consumer.State);

        await queueSpec.DeleteAsync();
        await _connection.CloseAsync();

        Assert.Contains((State.Open, State.Reconnecting), states);
        Assert.Contains((State.Reconnecting, State.Open), states);
        Assert.Contains((State.Open, State.Closing), states);
        Assert.Contains((State.Closing, State.Closed), states);
    }

    /// <summary>
    /// Simulate a case where the _connection is killed and producer and consumer are restarted
    /// After the _connection is killed, the producer and consumer should be restarted
    /// The test is easy and follow the happy path. To a more complex scenario, see the examples on the repository
    /// </summary>
    [Fact]
    public async Task PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();

        long messagesReceived = 0;

        IConsumer consumer = await _connection.ConsumerBuilder().InitialCredits(100).Queue(queueSpec)
            .MessageHandler(async (context, message) =>
            {
                Interlocked.Increment(ref messagesReceived);
                try
                {
                    await context.AcceptAsync();
                }
                catch (Exception)
                {
                    // ignored
                }
            }).BuildAsync();

        const int publishBatchCount = 10;
        int messagesConfirmed = 0;
        var message = new AmqpMessage("Hello World");
        var publishTasks = new List<Task<PublishResult>>();
        for (int i = 0; i < publishBatchCount; i++)
        {
            publishTasks.Add(publisher.PublishAsync(message));
        }

        await Task.WhenAll(publishTasks);

        foreach (Task<PublishResult> pt in publishTasks)
        {
            PublishResult pr = await pt;
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
            ++messagesConfirmed;
        }
        publishTasks.Clear();
        Assert.Equal(publishBatchCount, messagesConfirmed);

        await SystemUtils.WaitUntilFuncAsync(() => messagesReceived == 10);

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(_containerId);

        await SystemUtils.WaitUntilFuncAsync(() => publisher.State == State.Open);
        await SystemUtils.WaitUntilFuncAsync(() => consumer.State == State.Open);

        for (int i = 0; i < publishBatchCount; i++)
        {
            publishTasks.Add(publisher.PublishAsync(message));
        }

        await Task.WhenAll(publishTasks);

        foreach (Task<PublishResult> pt in publishTasks)
        {
            PublishResult pr = await pt;
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
            ++messagesConfirmed;
        }
        publishTasks.Clear();
        Assert.Equal(publishBatchCount * 2, messagesConfirmed);

        Assert.Equal(State.Open, publisher.State);

        await publisher.CloseAsync();

        await SystemUtils.WaitUntilFuncAsync(() => Interlocked.Read(ref messagesReceived) == 20);
        await consumer.CloseAsync();

        Assert.Equal(State.Closed, publisher.State);
        Assert.Equal(State.Closed, consumer.State);
    }

    /// <summary>
    /// The consumer and the publisher should not restart if the recovery is disabled
    /// </summary>
    [Fact]
    public async Task PublisherAndConsumerShouldNotRestartIfRecoveryIsDisabled()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        await _management.CloseAsync();
        _management.Dispose();
        await _connection.CloseAsync();
        _connection.Dispose();

        IRecoveryConfiguration recoveryConfiguration = RecoveryConfiguration.Create().Activated(false);
        ConnectionSettings connectionSettings =
            ConnectionSettingBuilder.Create().RecoveryConfiguration(recoveryConfiguration)
            .ContainerId(_containerId).Build();

        _connection = await AmqpConnection.CreateAsync(connectionSettings);
        _management = _connection.Management();

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();

        List<(State, State)> statesProducer = [];
        publisher.ChangeState += (sender, fromState, toState, e) =>
        {
            statesProducer.Add((fromState, toState));
        };

        IConsumer consumer = await _connection.ConsumerBuilder()
            .InitialCredits(100)
            .Queue(queueSpec)
            .MessageHandler(async (context, message) =>
            {
                try
                {
                    await context.AcceptAsync();
                }
                catch (Exception)
                {
                    // ignored
                }
            }).BuildAsync();

        List<(State, State)> statesConsumer = [];
        consumer.ChangeState += (sender, fromState, toState, e) =>
        {
            statesConsumer.Add((fromState, toState));
        };

        Assert.Equal(State.Open, publisher.State);
        Assert.Equal(State.Open, consumer.State);

        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);

        await SystemUtils.WaitUntilFuncAsync(() => publisher.State == State.Closed);
        await SystemUtils.WaitUntilFuncAsync(() => consumer.State == State.Closed);

        Assert.Equal(State.Closed, _connection.State);
        Assert.Equal(State.Closed, _management.State);

        Assert.DoesNotContain((State.Open, State.Closing), statesProducer);
        Assert.DoesNotContain((State.Closing, State.Closed), statesProducer);
        Assert.Contains((State.Open, State.Closed), statesProducer);

        Assert.DoesNotContain((State.Open, State.Closing), statesConsumer);
        Assert.DoesNotContain((State.Closing, State.Closed), statesConsumer);
        Assert.Contains((State.Open, State.Closed), statesConsumer);

        // Here we need a second _connection since the RecoveryConfiguration is disabled
        // and the _connection is closed. So we can't use the same _connection to delete the queue
        IConnection connection2 = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().RecoveryConfiguration(RecoveryConfiguration.Create().Activated(false))
                .ContainerId(_containerId).Build());

        IQueueSpecification queueSpec2 = connection2.Management().Queue(_queueName);
        await queueSpec2.DeleteAsync();
        await connection2.CloseAsync();
        connection2.Dispose();
    }
}
