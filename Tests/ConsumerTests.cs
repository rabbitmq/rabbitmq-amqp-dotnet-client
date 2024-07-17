using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class ConsumerTests(ITestOutputHelper testOutputHelper)
{
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;

    [Fact]
    public async Task SimpleConsumeMessage()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        await management.Queue().Name("SimpleConsumeMessage").Declare();
        var publisher = connection.PublisherBuilder().Queue("SimpleConsumeMessage").Build();
        await publisher.Publish(new AmqpMessage("Hello world!"),
            (_, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });

        TaskCompletionSource<IMessage> tcs = new();
        IConsumer consumer = connection.ConsumerBuilder().Queue("SimpleConsumeMessage").MessageHandler(
            (context, message) =>
            {
                context.Accept();
                tcs.SetResult(message);
            }
        ).Build();
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
        var publisher = connection.PublisherBuilder().Queue("ConsumerReQueueMessage").Build();
        await publisher.Publish(new AmqpMessage("Hello world!"),
            (_, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });

        TaskCompletionSource<int> tcs = new();
        int consumed = 0;
        IConsumer consumer = connection.ConsumerBuilder().Queue("ConsumerReQueueMessage").MessageHandler(
            (context, message) =>
            {
                Assert.Equal("Hello world!", message.Body());
                Interlocked.Increment(ref consumed);
                switch (consumed)
                {
                    case 1:
                        // first time requeue the message
                        // it must consume again
                        context.Requeue();
                        break;
                    case 2:
                        context.Accept();
                        tcs.SetResult(consumed);
                        break;
                }
            }
        ).Build();
        await Task.WhenAny(tcs.Task, Task.Delay(5000));
        Assert.True(tcs.Task.IsCompleted);
        await consumer.CloseAsync();
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("ConsumerReQueueMessage") == 0, 500);
        await management.QueueDeletion().Delete("ConsumerReQueueMessage");
        await connection.CloseAsync();
    }

    [Fact]
    public async Task ConsumerRejectOnlySomeMessage()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Queue().Name("ConsumerRejectOnlySomeMessage").Declare();
        var publisher = connection.PublisherBuilder().Queue("ConsumerRejectOnlySomeMessage").Build();

        for (int i = 0; i < 500; i++)
        {
            await publisher.Publish(new AmqpMessage($"message_{i}"),
                (_, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });
        }

        TaskCompletionSource<List<IMessage>> tcs = new();
        int consumed = 0;

        List<IMessage> receivedMessages = new();
        IConsumer consumer = connection.ConsumerBuilder().Queue("ConsumerRejectOnlySomeMessage").InitialCredits(100)
            .MessageHandler((
                context, message) =>
            {
                receivedMessages.Add(message);
                Interlocked.Increment(ref consumed);
                if (consumed % 2 == 0)
                {
                    context.Discard();
                }
                else
                {
                    context.Accept();
                }

                if (consumed == 500)
                {
                    tcs.SetResult(receivedMessages);
                }
            }).Build();

        await Task.WhenAny(tcs.Task, Task.Delay(5000));

        Assert.True(tcs.Task.IsCompleted);

        List<IMessage> messages = await tcs.Task;

        Assert.Equal(500, messages.Count);


        for (int i = 0; i < 500; i++)
        {
            if (i % 2 == 0)
            {
                Assert.Equal($"message_{i}", messages[i].Body());
            }
        }

        await consumer.CloseAsync();
        await management.QueueDeletion().Delete("ConsumerRejectOnlySomeMessage");
        await connection.CloseAsync();
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
        IConsumer consumer = connection.ConsumerBuilder().Queue(queueName).InitialCredits(100)
            .MessageHandler((context, message) => { Interlocked.Increment(ref consumed); }).Stream().Offset(offset)
            .Builder().Build();

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

    private static async Task Publish(IConnection connection, string queue, int numberOfMessages,
        string filter = null)
    {
        var publisher = connection.PublisherBuilder().Queue(queue).Build();
        for (int i = 0; i < numberOfMessages; i++)
        {
            IMessage message = new AmqpMessage($"message_{i}");
            if (filter != null)
            {
                message.Annotation("x-stream-filter-value", filter);
            }

            await publisher.Publish(new AmqpMessage($"message_{i}"),
                (_, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });
        }
    }
}
