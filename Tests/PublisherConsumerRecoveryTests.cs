using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class PublisherConsumerRecoveryTests(ITestOutputHelper testOutputHelper)
{
    private ITestOutputHelper TestOutputHelper { get; } = testOutputHelper;

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
        List<(State,State)> states = [];
        publisher.ChangeState += (sender, fromState, toState, e) =>
        {
            states.Add((fromState, toState));
        };
        
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
        List<(State,State)> states = [];
        consumer.ChangeState += (sender, fromState, toState, e) =>
        {
            states.Add((fromState, toState));
        };
        
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
        IPublisher publisher = connection.PublisherBuilder().Queue("ProducerShouldChangeStatusWhenConnectionIsKilled").Build();
        List<(State,State)> states = [];
        publisher.ChangeState += (sender, fromState, toState, e) =>
        {
            states.Add((fromState, toState));
        };
        
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
        IConsumer consumer = connection.ConsumerBuilder().Queue("ConsumerShouldChangeStatusWhenConnectionIsKilled").Build();
        List<(State,State)> states = [];
        consumer.ChangeState += (sender, fromState, toState, e) =>
        {
            states.Add((fromState, toState));
        };
        
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
    
    
    
    
    
    
}
