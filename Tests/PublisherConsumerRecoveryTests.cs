using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests;

public class PublisherConsumerRecoveryTests()
{
    /// <summary>
    /// Test the Simple case where the producer is closed and the status is changed
    /// </summary>
    [Fact]
    public async Task ProducerShouldChangeStatusWhenClosed()
    {
        string containerId = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(containerId).Build());
        await connection.Management().Queue().Name("ProducerShouldChangeStatusWhenClosed").Declare();

        IPublisher publisher = await connection.PublisherBuilder().Queue("ProducerShouldChangeStatusWhenClosed").BuildAsync();

        List<(State, State)> states = [];
        publisher.ChangeState += (sender, fromState, toState, e) => { states.Add((fromState, toState)); };

        Assert.Equal(State.Open, publisher.State);
        await publisher.CloseAsync();
        Assert.Equal(State.Closed, publisher.State);
        await connection.Management().QueueDeletion().Delete("ProducerShouldChangeStatusWhenClosed");
        await connection.CloseAsync();
        Assert.Contains((State.Open, State.Closing), states);
        Assert.Contains((State.Closing, State.Closed), states);
    }


    /// <summary>
    /// Test the Simple case where the consumer is closed and the status is changed
    /// </summary>
    [Fact]
    public async Task ConsumerShouldChangeStatusWhenClosed()
    {
        string containerId = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(containerId).Build());
        await connection.Management().Queue().Name("ConsumerShouldChangeStatusWhenClosed").Declare();
        IConsumer consumer = await connection.ConsumerBuilder().Queue("ConsumerShouldChangeStatusWhenClosed")
            .MessageHandler((context, message) => { return Task.CompletedTask; }).BuildAsync();
        List<(State, State)> states = [];
        consumer.ChangeState += (sender, fromState, toState, e) => { states.Add((fromState, toState)); };

        Assert.Equal(State.Open, consumer.State);
        await consumer.CloseAsync();
        Assert.Equal(State.Closed, consumer.State);
        await connection.Management().QueueDeletion().Delete("ConsumerShouldChangeStatusWhenClosed");
        await connection.CloseAsync();
        Assert.Contains((State.Open, State.Closing), states);
        Assert.Contains((State.Closing, State.Closed), states);
    }


    /// <summary>
    /// Test the case where the connection is killed and the producer status is changed
    /// In this case the `states` list should contain the following states:
    /// - Open -> Reconnecting
    /// - Reconnecting -> Open
    /// - Open -> Closing
    /// - Closing -> Closed
    /// </summary>
    [Fact]
    public async Task ProducerShouldChangeStatusWhenConnectionIsKilled()
    {
        string containerId = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(containerId).Build());
        await connection.Management().Queue().Name("ProducerShouldChangeStatusWhenConnectionIsKilled").Declare();

        IPublisher publisher = await connection.PublisherBuilder().Queue("ProducerShouldChangeStatusWhenConnectionIsKilled")
            .BuildAsync();

        List<(State, State)> states = [];
        publisher.ChangeState += (sender, fromState, toState, e) => { states.Add((fromState, toState)); };

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(containerId);

        await SystemUtils.WaitUntilFuncAsync(() => publisher.State == State.Open);

        Assert.Equal(State.Open, publisher.State);
        await publisher.CloseAsync();
        Assert.Equal(State.Closed, publisher.State);
        await connection.Management().QueueDeletion().Delete("ProducerShouldChangeStatusWhenConnectionIsKilled");
        await connection.CloseAsync();

        Assert.Contains((State.Open, State.Reconnecting), states);
        Assert.Contains((State.Reconnecting, State.Open), states);
        Assert.Contains((State.Open, State.Closing), states);
        Assert.Contains((State.Closing, State.Closed), states);
    }

    /// <summary>
    /// Test the case where the connection is killed and the consumer status is changed
    /// In this case the `states` list should contain the following states:
    /// - Open -> Reconnecting
    /// - Reconnecting -> Open
    /// - Open -> Closing
    /// - Closing -> Closed
    /// </summary>
    [Fact]
    public async Task ConsumerShouldChangeStatusWhenConnectionIsKilled()
    {
        string containerId = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(containerId).Build());
        await connection.Management().Queue().Name("ConsumerShouldChangeStatusWhenConnectionIsKilled").Declare();

        IConsumer consumer = await connection.ConsumerBuilder().Queue("ConsumerShouldChangeStatusWhenConnectionIsKilled")
            .MessageHandler((context, message) => { return Task.CompletedTask; })
            .BuildAsync();

        List<(State, State)> states = [];
        consumer.ChangeState += (sender, fromState, toState, e) => { states.Add((fromState, toState)); };

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(containerId);

        await SystemUtils.WaitUntilFuncAsync(() => consumer.State == State.Open);

        Assert.Equal(State.Open, consumer.State);
        await consumer.CloseAsync();
        Assert.Equal(State.Closed, consumer.State);
        await connection.Management().QueueDeletion().Delete("ConsumerShouldChangeStatusWhenConnectionIsKilled");
        await connection.CloseAsync();

        Assert.Contains((State.Open, State.Reconnecting), states);
        Assert.Contains((State.Reconnecting, State.Open), states);
        Assert.Contains((State.Open, State.Closing), states);
        Assert.Contains((State.Closing, State.Closed), states);
    }


    /// <summary>
    /// Simulate a case where the connection is killed and producer and consumer are restarted
    /// After the connection is killed, the producer and consumer should be restarted
    /// The test is easy and follow the happy path. To a more complex scenario, see the examples on the repository
    /// </summary>
    [Fact]
    public async Task PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled()
    {
        const string queueName = nameof(PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled);

        string containerId = Guid.NewGuid().ToString();

        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ContainerId(containerId).Build());

        await connection.Management().Queue().Name(queueName).Declare();

        IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

        long messagesReceived = 0;

        IConsumer consumer = await connection.ConsumerBuilder().InitialCredits(100).Queue(queueName)
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

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(containerId);

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

        await connection.Management().QueueDeletion().Delete(queueName);
        await connection.CloseAsync();
    }

    /// <summary>
    /// The consumer and the publisher should not restart if the recovery is disabled
    /// </summary>
    [Fact]
    public async Task PublisherAndConsumerShouldNotRestartIfRecoveryIsDisabled()
    {
        const string queueName = nameof(PublisherAndConsumerShouldNotRestartIfRecoveryIsDisabled);

        string containerId = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().RecoveryConfiguration(RecoveryConfiguration.Create().Activated(false))
                .ContainerId(containerId).Build());

        await connection.Management().Queue().Name(queueName).Declare();

        IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

        List<(State, State)> statesProducer = [];
        publisher.ChangeState += (sender, fromState, toState, e) => { statesProducer.Add((fromState, toState)); };

        IConsumer consumer = await connection.ConsumerBuilder().InitialCredits(100).Queue(queueName)
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
        consumer.ChangeState += (sender, fromState, toState, e) => { statesConsumer.Add((fromState, toState)); };

        Assert.Equal(State.Open, publisher.State);
        Assert.Equal(State.Open, consumer.State);

        await SystemUtils.WaitUntilConnectionIsKilled(containerId);
        await SystemUtils.WaitUntilFuncAsync(() => publisher.State == State.Closed);
        await SystemUtils.WaitUntilFuncAsync(() => consumer.State == State.Closed);

        Assert.Equal(State.Closed, connection.State);
        Assert.Equal(State.Closed, connection.Management().State);
        Assert.DoesNotContain((State.Open, State.Closing), statesProducer);
        Assert.DoesNotContain((State.Closing, State.Closed), statesProducer);
        Assert.Contains((State.Open, State.Closed), statesProducer);

        Assert.DoesNotContain((State.Open, State.Closing), statesConsumer);
        Assert.DoesNotContain((State.Closing, State.Closed), statesConsumer);
        Assert.Contains((State.Open, State.Closed), statesConsumer);

        // Here we need a second connection since the RecoveryConfiguration is disabled
        // and the connection is closed. So we can't use the same connection to delete the queue
        IConnection connection2 = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().RecoveryConfiguration(RecoveryConfiguration.Create().Activated(false))
                .ContainerId(containerId).Build());

        await connection2.Management().QueueDeletion().Delete(queueName);
        await connection2.CloseAsync();
    }
}
