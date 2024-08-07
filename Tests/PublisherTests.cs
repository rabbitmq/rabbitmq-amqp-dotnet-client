using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class PublisherTests(ITestOutputHelper testOutputHelper)
{
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;

    [Fact]
    public async Task ValidateBuilderRaiseExceptionIfQueueOrExchangeAreNotSetCorrectly()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());

        await Assert.ThrowsAsync<InvalidAddressException>(() =>
            connection.PublisherBuilder().Queue("does_not_matter").Exchange("i_should_not_stay_here").BuildAsync());

        await Assert.ThrowsAsync<InvalidAddressException>(() => connection.PublisherBuilder().Exchange("").BuildAsync());

        await Assert.ThrowsAsync<InvalidAddressException>(() => connection.PublisherBuilder().Queue("").BuildAsync());

        Assert.Empty(connection.GetPublishers());

        await connection.CloseAsync();
    }

    [Fact]
    public async Task RaiseErrorIfQueueDoesNotExist()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());

        await Assert.ThrowsAsync<PublisherException>(() =>
            connection.PublisherBuilder().Queue("queue_does_not_exist").BuildAsync());

        await connection.CloseAsync();
    }

    [Fact]
    public async Task SendAMessageToAQueue()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Queue().Name("queue_to_send").Declare();

        IPublisher publisher = await connection.PublisherBuilder().Queue("queue_to_send").BuildAsync();

        await publisher.Publish(new AmqpMessage("Hello wold!"),
            (message, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });

        await SystemUtils.WaitUntilQueueMessageCount("queue_to_send", 1);

        Assert.Single(connection.GetPublishers());
        await publisher.CloseAsync();
        Assert.Empty(connection.GetPublishers());
        await management.QueueDeletion().Delete("queue_to_send");
        await connection.CloseAsync();
    }


    [Fact]
    public async Task ValidatePublishersCount()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Queue().Name("queue_publishers_count").Declare();

        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int received = 0;
        for (int i = 1; i <= 10; i++)
        {
            IPublisher publisher = await connection.PublisherBuilder().Queue("queue_publishers_count").BuildAsync();

            await publisher.Publish(new AmqpMessage("Hello wold!"),
                (message, descriptor) =>
                {
                    Assert.Equal(OutcomeState.Accepted, descriptor.State);
                    if (Interlocked.Increment(ref received) == 10)
                    {
                        tcs.SetResult(true);
                    }
                });
            Assert.Equal(i, connection.GetPublishers().Count);
        }

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        foreach (IPublisher publisher in connection.GetPublishers())
        {
            await publisher.CloseAsync();
        }

        await management.QueueDeletion().Delete("queue_publishers_count");
        await connection.CloseAsync();
        Assert.Empty(connection.GetPublishers());
    }

    [Fact]
    public async Task SendAMessageToAnExchange()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Queue().Name("queue_to_send_1").Declare();
        await management.Exchange().Name("exchange_to_send").Declare();
        await management.Binding().SourceExchange("exchange_to_send").DestinationQueue("queue_to_send_1").Key("key")
            .Bind();

        IPublisher publisher = await connection.PublisherBuilder().Exchange("exchange_to_send").Key("key").BuildAsync();

        await publisher.Publish(new AmqpMessage("Hello wold!"),
            (message, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });

        await SystemUtils.WaitUntilQueueMessageCount("queue_to_send_1", 1);

        Assert.Single(connection.GetPublishers());
        await publisher.CloseAsync();
        Assert.Empty(connection.GetPublishers());

        await management.Binding().SourceExchange("exchange_to_send").DestinationQueue("queue_to_send_1").Key("key")
            .Unbind();
        await management.ExchangeDeletion().Delete("exchange_to_send");
        await management.QueueDeletion().Delete("queue_to_send_1");
        await connection.CloseAsync();
    }
}
