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

        SystemUtils.WaitUntil(() => publisher.State == State.Open);

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

        SystemUtils.WaitUntil(() => consumer.State == State.Open);

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


    [Fact]
    public async Task PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled()
    {
        string connectionName = Guid.NewGuid().ToString();
        IConnection connection = await AmqpConnection.CreateAsync(
            ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build());
        await connection.Management().Queue()
            .Name("PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled").Declare();

        IPublisher publisher = connection.PublisherBuilder()
            .Queue("PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled"). Build();

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

        SystemUtils.WaitUntil(() => messagesConfirmed == 10);
        SystemUtils.WaitUntil(() => messagesReceived == 10);
        
        await SystemUtils.WaitUntilConnectionIsKilledAndOpen(connectionName);

        SystemUtils.WaitUntil(() => publisher.State == State.Open);
        SystemUtils.WaitUntil(() => consumer.State == State.Open);

        for (int i = 0; i < 10; i++)
        {
            await publisher.Publish(new AmqpMessage("Hello World"),
                (message, descriptor) =>
                {
                    Interlocked.Increment(ref messagesConfirmed);
                    Assert.Equal(OutcomeState.Accepted, descriptor.State);
                });
        }

        SystemUtils.WaitUntil(() => messagesConfirmed == 20);
        Assert.Equal(State.Open, publisher.State);

        await publisher.CloseAsync();
        await consumer.CloseAsync();

        Assert.Equal(State.Closed, publisher.State);
        Assert.Equal(State.Closed, consumer.State);

        await connection.Management().QueueDeletion()
            .Delete("PublishShouldRestartPublishConsumerShouldRestartConsumeWhenConnectionIsKilled");
        await connection.CloseAsync();
        SystemUtils.WaitUntil(() => messagesReceived == 20);
    }
}
