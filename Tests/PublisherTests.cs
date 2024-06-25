using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests;

public class PublisherTests
{
    [Fact]
    public async void ValidateBuilderRaiseExceptionIfQueueOrExchangeAreNotSetCorrectly()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        Assert.Throws<InvalidAddressException>(() =>
            connection.PublisherBuilder().Queue("does_not_matter").Exchange("i_should_not_stay_here").Build());
        Assert.Throws<InvalidAddressException>(() => connection.PublisherBuilder().Exchange("").Build());
        Assert.Throws<InvalidAddressException>(() => connection.PublisherBuilder().Queue("").Build());
        Assert.Empty(connection.GetPublishers());
        await connection.CloseAsync();
    }


    [Fact]
    public async void RaiseExceptionIfQueueDoesNotExist()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var publisher = connection.PublisherBuilder().Queue("queue_does_not_exist").Build();
        await Assert.ThrowsAsync<PublisherException>(async () =>
            await publisher.Publish(new AmqpMessage("Hello Null!")));
        await connection.CloseAsync();
    }

    [Fact]
    public async void SendAMessageToAQueue()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        await management.Queue().Name("queue_to_send").Declare();
        var publisher = connection.PublisherBuilder().Queue("queue_to_send").Build();
        await publisher.Publish(new AmqpMessage("Hello wold!"));
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("queue_to_send") == 1);
        Assert.Single(connection.GetPublishers());
        await publisher.CloseAsync();
        Assert.Empty(connection.GetPublishers());
        await management.QueueDeletion().Delete("queue_to_send");
        await connection.CloseAsync();
    }


    [Fact]
    public async void ValidatePublishersCount()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        await management.Queue().Name("queue_publishers_count").Declare();

        for (var i = 1; i <= 10; i++)
        {
            var publisher = connection.PublisherBuilder().Queue("queue_publishers_count").Build();
            await publisher.Publish(new AmqpMessage("Hello wold!"));
            Assert.Equal(i, connection.GetPublishers().Count);
        }

        await management.QueueDeletion().Delete("queue_publishers_count");
        await connection.CloseAsync();
        Assert.Empty(connection.GetPublishers());
    }
    
}