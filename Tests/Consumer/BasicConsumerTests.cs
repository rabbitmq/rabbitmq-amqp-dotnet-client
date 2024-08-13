using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

public class BasicConsumerTests(ITestOutputHelper testOutputHelper)
{
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;

    [Fact]
    public async Task SimpleConsumeMessage()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Queue().Name("SimpleConsumeMessage").Declare();

        IPublisher publisher = await connection.PublisherBuilder().Queue("SimpleConsumeMessage").BuildAsync();

        var message = new AmqpMessage("Hello world!");
        PublishResult pr = await publisher.PublishAsync(message);
        Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

        TaskCompletionSource<IMessage> tcs = new();
        IConsumer consumer = await connection.ConsumerBuilder().Queue("SimpleConsumeMessage").MessageHandler(
            async (context, message) =>
            {
                await context.AcceptAsync();
                tcs.SetResult(message);
            }
        ).BuildAsync();

        await Task.WhenAny(tcs.Task, Task.Delay(5000));
        Assert.True(tcs.Task.IsCompleted);
        IMessage receivedMessage = await tcs.Task;
        Assert.Equal("Hello world!", receivedMessage.Body());
        await consumer.CloseAsync();
        await management.QueueDeletion().Delete("SimpleConsumeMessage");
        await connection.CloseAsync();
    }


    /// <summary>
    /// Test the Requeue method of the IContext interface
    /// The first time the message is requeued, the second time it is accepted
    /// It is a bit tricky to test the requeue method, because the message is requeued asynchronously
    /// but this simple use case should be enough to test the requeue method
    /// </summary>
    [Fact]
    public async Task ConsumerReQueueMessage()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Queue().Name("ConsumerReQueueMessage").Declare();

        IPublisher publisher = await connection.PublisherBuilder().Queue("ConsumerReQueueMessage").BuildAsync();

        var message = new AmqpMessage("Hello world!");
        PublishResult pr = await publisher.PublishAsync(message);
        Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

        TaskCompletionSource<int> tcs = new();
        int consumed = 0;
        IConsumer consumer = await connection.ConsumerBuilder().Queue("ConsumerReQueueMessage").MessageHandler(
            async (context, message) =>
            {
                Assert.Equal("Hello world!", message.Body());
                Interlocked.Increment(ref consumed);
                switch (consumed)
                {
                    case 1:
                        // first time requeue the message
                        // it must consume again
                        await context.RequeueAsync();
                        break;
                    case 2:
                        await context.AcceptAsync();
                        tcs.SetResult(consumed);
                        break;
                }
            }
        ).BuildAsync();

        await Task.WhenAny(tcs.Task, Task.Delay(10000));
        Assert.True(tcs.Task.IsCompleted);
        await consumer.CloseAsync();

        await SystemUtils.WaitUntilQueueMessageCount("ConsumerReQueueMessage", 0);
        await management.QueueDeletion().Delete("ConsumerReQueueMessage");
        await connection.CloseAsync();
    }

    [Fact]
    public async Task ConsumerRejectOnlySomeMessage()
    {
        const int publishCount = 500;
        const int initialCredits = 100;
        const string queueName = nameof(ConsumerRejectOnlySomeMessage);

        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        IQueueSpecification queueSpec = management.Queue(queueName);
        await queueSpec.Declare();

        IPublisher? publisher = null;
        IConsumer? consumer = null;
        try
        {
            publisher = await connection.PublisherBuilder().Queue("ConsumerRejectOnlySomeMessage").BuildAsync();

            var publishTasks = new List<Task<PublishResult>>();
            for (int i = 0; i < publishCount; i++)
            {
                var message = new AmqpMessage($"message_{i}");
                publishTasks.Add(publisher.PublishAsync(message));
            }
            await Task.WhenAll(publishTasks);
            foreach (Task<PublishResult> pt in publishTasks)
            {
                PublishResult pr = await pt;
                Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
            }

            TaskCompletionSource<List<IMessage>> tcs = new();
            int messagesConsumedCount = 0;
            List<IMessage> receivedMessages = new();
            async Task MessageHandler(IContext cxt, IMessage msg)
            {
                receivedMessages.Add(msg);

                Interlocked.Increment(ref messagesConsumedCount);

                if (messagesConsumedCount % 2 == 0)
                {
                    await cxt.DiscardAsync();
                }
                else
                {
                    await cxt.AcceptAsync();
                }

                if (messagesConsumedCount == publishCount)
                {
                    tcs.SetResult(receivedMessages);
                }
            }

            consumer = await connection.ConsumerBuilder().Queue("ConsumerRejectOnlySomeMessage").InitialCredits(initialCredits)
                .MessageHandler(MessageHandler).BuildAsync();

            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(tcs.Task.IsCompleted);
            List<IMessage> receivedMessagesFromTask = await tcs.Task;

            Assert.Equal(publishCount, receivedMessagesFromTask.Count);

            for (int i = 0; i < publishCount; i++)
            {
                if (i % 2 == 0)
                {
                    Assert.Equal($"message_{i}", receivedMessagesFromTask[i].Body());
                }
            }
        }
        finally
        {
            if (publisher is not null)
            {
                await publisher.CloseAsync();
            }

            if (consumer is not null)
            {
                await consumer.CloseAsync();
            }

            await management.QueueDeletion().Delete(queueName);
            await connection.CloseAsync();
        }
    }

    /// <summary>
    /// Test the consumer for a stream queue with offset
    /// The test is not deterministic because we don't know how many messages will be consumed
    /// We assume that the messages consumed are greater than or equal to the expected number of messages
    /// </summary>
    /// <param name="offset"></param>
    /// <param name="numberOfMessagesExpected"></param>
    [Theory]
    [InlineData(StreamOffsetSpecification.First, 100)]
    [InlineData(StreamOffsetSpecification.Last, 1)]
    [InlineData(StreamOffsetSpecification.Next, 0)]
    public async Task ConsumerForStreamQueueWithOffset(StreamOffsetSpecification offset, int numberOfMessagesExpected)
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        string queueName = $"ConsumerForStreamQueueWithOffset_{offset}";
        await management.Queue().Name(queueName).Type(QueueType.STREAM).Declare();
        await Publish(connection, queueName, 100);
        int consumed = 0;
        IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).InitialCredits(100)
            .MessageHandler((context, message) =>
            {
                Interlocked.Increment(ref consumed);
                return Task.CompletedTask;
            }).Stream().Offset(offset)
            .Builder().BuildAsync();

        // wait for the consumer to consume all messages
        // we can't use the TaskCompletionSource here because we don't know how many messages will be consumed
        // In two seconds, the consumer should consume all messages
        await Task.Delay(2000);

        // we don't know how many messages will be consumed
        // expect for the case FIRST
        // we just assume that the messages consumed are greater than or equal to the expected number of messages
        // For example in case of "LAST" we expect 1 message to be consumed, but it could be more
        Assert.True(consumed >= numberOfMessagesExpected);
        await consumer.CloseAsync();
        await management.QueueDeletion().Delete(queueName);
        await connection.CloseAsync();
    }


    /// <summary>
    /// Test for stream filtering
    /// There are two consumers:
    /// - one with a filter that should receive only the messages with the filter
    /// - one without filter that should receive all messages
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="expected"></param>
    [Theory]
    [InlineData("pizza,beer,pasta,wine", 4)]
    [InlineData("pizza,beer", 2)]
    [InlineData("pizza", 1)]
    public async Task ConsumerWithStreamFilterShouldReceiveOnlyPartOfTheMessages(string filter, int expected)
    {
        string[] filters = filter.Split(",");

        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        string queueName = $"ConsumerWithStreamFilterShouldReceiveOnlyPartOfTheMessages_{filter}";
        await management.Queue().Name(queueName).Type(QueueType.STREAM).Declare();
        foreach (string se in filters)
        {
            await Publish(connection, queueName, 1, se);
        }

        // wait for the messages to be published and the chunks to be created
        await Task.Delay(1000);
        // publish extra messages without filter and these messages should be always excluded
        // by the consumer with the filter
        await Publish(connection, queueName, 10);

        List<IMessage> receivedMessages = [];
        IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).InitialCredits(100)
            .MessageHandler((context, message) =>
            {
                receivedMessages.Add(message);
                return context.AcceptAsync();
            }).Stream().FilterValues(filters).FilterMatchUnfiltered(false)
            .Offset(StreamOffsetSpecification.First).Builder()
            .BuildAsync();

        int receivedWithoutFilters = 0;
        IConsumer consumerWithoutFilters = await connection.ConsumerBuilder().Queue(queueName).InitialCredits(100)
            .MessageHandler((context, message) =>
            {
                Interlocked.Increment(ref receivedWithoutFilters);
                return context.AcceptAsync();
            }).Stream()
            .Offset(StreamOffsetSpecification.First).Builder()
            .BuildAsync();

        // wait for the consumer to consume all messages
        await Task.Delay(500);
        Assert.Equal(expected, receivedMessages.Count);
        Assert.Equal(filters.Length + 10, receivedWithoutFilters);

        await consumer.CloseAsync();
        await consumerWithoutFilters.CloseAsync();
        await management.QueueDeletion().Delete(queueName);
        await connection.CloseAsync();
    }


    /// <summary>
    /// Test the offset value for the stream queue
    /// </summary>
    /// <param name="offsetStart"></param>
    /// <param name="numberOfMessagesExpected"></param>
    [Theory]
    [InlineData(0, 100)]
    [InlineData(50, 50)]
    [InlineData(99, 1)]
    public async Task ConsumerForStreamQueueWithOffsetValue(int offsetStart, int numberOfMessagesExpected)
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        string queueName = $"ConsumerForStreamQueueWithOffsetValue_{offsetStart}";
        await management.Queue().Name(queueName).Type(QueueType.STREAM).Declare();
        await Publish(connection, queueName, 100);
        int consumed = 0;
        IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).InitialCredits(100)
            .MessageHandler((context, message) =>
            {
                Interlocked.Increment(ref consumed);
                return Task.CompletedTask;
            }).Stream().Offset(offsetStart)
            .Builder().BuildAsync();

        // wait for the consumer to consume all messages
        // we can't use the TaskCompletionSource here because we don't know how many messages will be consumed
        // In two seconds, the consumer should consume all messages
        await Task.Delay(2000);

        Assert.Equal(consumed, numberOfMessagesExpected);
        await consumer.CloseAsync();
        await management.QueueDeletion().Delete(queueName);
        await connection.CloseAsync();
    }

    private static async Task Publish(IConnection connection, string queue, int numberOfMessages,
        string? filter = null)
    {
        IPublisher publisher = await connection.PublisherBuilder().Queue(queue).BuildAsync();

        var publishTasks = new List<Task<PublishResult>>();
        for (int i = 0; i < numberOfMessages; i++)
        {
            IMessage message = new AmqpMessage($"message_{i}");
            if (filter != null)
            {
                message.Annotation("x-stream-filter-value", filter);
            }

            publishTasks.Add(publisher.PublishAsync(message));
        }
        await Task.WhenAll(publishTasks);
        foreach (Task<PublishResult> pt in publishTasks)
        {
            PublishResult pr = await pt;
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
        }
    }
}
