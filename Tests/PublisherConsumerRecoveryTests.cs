using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class PublisherConsumerRecoveryTests(ITestOutputHelper testOutputHelper)
{
    /// <summary>
    /// Test the Simple case where the producer is closed and the status is changed
    /// </summary>
    [Fact]
    public async Task ProducerShouldChangeStatusWhenClosed()
    {
        string connectionName = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build());
        await connection.Management().Queue().Name("ProducerShouldChangeStatusWhenClosed").Declare();
        IPublisher publisher = connection.PublisherBuilder().Queue("ProducerShouldChangeStatusWhenClosed").Build();
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
        string connectionName = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build());
        await connection.Management().Queue().Name("ConsumerShouldChangeStatusWhenClosed").Declare();
        IConsumer consumer = connection.ConsumerBuilder().Queue("ConsumerShouldChangeStatusWhenClosed").Build();
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
        string connectionName = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build());
        await connection.Management().Queue().Name("ProducerShouldChangeStatusWhenConnectionIsKilled").Declare();
        IPublisher publisher = connection.PublisherBuilder().Queue("ProducerShouldChangeStatusWhenConnectionIsKilled")
            .Build();
        List<(State, State)> states = [];
        publisher.ChangeState += (sender, fromState, toState, e) => { states.Add((fromState, toState)); };

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(connectionName);

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
        string connectionName = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build());
        await connection.Management().Queue().Name("ConsumerShouldChangeStatusWhenConnectionIsKilled").Declare();
        IConsumer consumer = connection.ConsumerBuilder().Queue("ConsumerShouldChangeStatusWhenConnectionIsKilled")
            .Build();
        List<(State, State)> states = [];
        consumer.ChangeState += (sender, fromState, toState, e) => { states.Add((fromState, toState)); };

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(connectionName);

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
        string connectionName = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build());
        await connection.Management().Queue()
            .Name("PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled").Declare();

        IPublisher publisher = connection.PublisherBuilder()
            .Queue("PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled").Build();

        int messagesReceived = 0;
        IConsumer consumer = connection.ConsumerBuilder().InitialCredits(100)
            .Queue("PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled")
            .MessageHandler((context, message) =>
            {
                Interlocked.Increment(ref messagesReceived);
                testOutputHelper.WriteLine($"Received message {messagesReceived}");
                try
                {
                    context.Accept();
                }
                catch (Exception)
                {
                    // ignored
                }
            }).Build();
        int messagesConfirmed = 0;
        for (int i = 0; i < 10; i++)
        {
            await publisher.Publish(new AmqpMessage("Hello World"),
                (message, descriptor) =>
                {
                    Assert.Equal(OutcomeState.Accepted, descriptor.State);
                    Interlocked.Increment(ref messagesConfirmed);
                });
        }

        await SystemUtils.WaitUntilFuncAsync(() => messagesConfirmed == 10);
        await SystemUtils.WaitUntilFuncAsync(() => messagesReceived == 10);

        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(connectionName);

        await SystemUtils.WaitUntilFuncAsync(() => publisher.State == State.Open);
        await SystemUtils.WaitUntilFuncAsync(() => consumer.State == State.Open);

        for (int i = 0; i < 10; i++)
        {
            await publisher.Publish(new AmqpMessage("Hello World"),
                (message, descriptor) =>
                {
                    Interlocked.Increment(ref messagesConfirmed);
                    Assert.Equal(OutcomeState.Accepted, descriptor.State);
                });
        }

        await SystemUtils.WaitUntilFuncAsync(() => messagesConfirmed == 20);
        Assert.Equal(State.Open, publisher.State);

        await publisher.CloseAsync();
        await consumer.CloseAsync();

        Assert.Equal(State.Closed, publisher.State);
        Assert.Equal(State.Closed, consumer.State);

        await connection.Management().QueueDeletion()
            .Delete("PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled");
        await connection.CloseAsync();
        await SystemUtils.WaitUntilFuncAsync(() => messagesReceived == 20);
    }

    /// <summary>
    /// The consumer and the publisher should not restart if the recovery is disabled
    /// </summary>
    [Fact]
    public async Task PublisherAndConsumerShouldNotRestartIfRecoveryIsDisabled()
    {
        string connectionName = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().RecoveryConfiguration(RecoveryConfiguration.Create().Activated(false))
                .ConnectionName(connectionName).Build());

        await connection.Management().Queue().Name("PublisherAndConsumerShouldNotRestartIfRecoveryIsDisabled")
            .Declare();

        IPublisher publisher = connection.PublisherBuilder()
            .Queue("PublisherAndConsumerShouldNotRestartIfRecoveryIsDisabled").Build();

        List<(State, State)> statesProducer = [];
        publisher.ChangeState += (sender, fromState, toState, e) => { statesProducer.Add((fromState, toState)); };


        IConsumer consumer = connection.ConsumerBuilder().InitialCredits(100)
            .Queue("PublisherAndConsumerShouldNotRestartIfRecoveryIsDisabled")
            .MessageHandler((context, message) =>
            {
                try
                {
                    context.Accept();
                }
                catch (Exception)
                {
                    // ignored
                }
            }).Build();

        List<(State, State)> statesConsumer = [];
        consumer.ChangeState += (sender, fromState, toState, e) => { statesConsumer.Add((fromState, toState)); };

        Assert.Equal(State.Open, publisher.State);
        Assert.Equal(State.Open, consumer.State);

        await SystemUtils.WaitUntilConnectionIsKilled(connectionName);
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
                .ConnectionName(connectionName).Build());

        await connection2.Management().QueueDeletion()
            .Delete("PublisherAndConsumerShouldNotRestartIfRecoveryIsDisabled");

        await connection2.CloseAsync();
    }
}
