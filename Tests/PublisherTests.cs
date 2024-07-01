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
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        Assert.Throws<InvalidAddressException>(() =>
            connection.PublisherBuilder().Queue("does_not_matter").Exchange("i_should_not_stay_here").Build());
        Assert.Throws<InvalidAddressException>(() => connection.PublisherBuilder().Exchange("").Build());
        Assert.Throws<InvalidAddressException>(() => connection.PublisherBuilder().Queue("").Build());
        Assert.Empty(connection.GetPublishers());
        await connection.CloseAsync();
    }


    [Fact]
    public async Task RaiseErrorIfQueueDoesNotExist()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        Assert.Throws<PublisherException>(() =>
           connection.PublisherBuilder().Queue("queue_does_not_exist").Build());

        await connection.CloseAsync();
    }

    [Fact]
    public async Task SendAMessageToAQueue()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        await management.Queue().Name("queue_to_send").Declare();
        var publisher = connection.PublisherBuilder().Queue("queue_to_send").Build();
        await publisher.Publish(new AmqpMessage("Hello wold!"),
            (message, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("queue_to_send") == 1);
        Assert.Single(connection.GetPublishers());
        await publisher.CloseAsync();
        Assert.Empty(connection.GetPublishers());
        await management.QueueDeletion().Delete("queue_to_send");
        await connection.CloseAsync();
    }


    [Fact]
    public async Task ValidatePublishersCount()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        await management.Queue().Name("queue_publishers_count").Declare();

        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = 0;
        for (var i = 1; i <= 10; i++)
        {
            var publisher = connection.PublisherBuilder().Queue("queue_publishers_count").Build();
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
        foreach (var publisher in connection.GetPublishers())
        {
            _testOutputHelper.WriteLine(publisher.Id);
            await publisher.CloseAsync();
        }

        await management.QueueDeletion().Delete("queue_publishers_count");
        await connection.CloseAsync();
        Assert.Empty(connection.GetPublishers());
    }
}