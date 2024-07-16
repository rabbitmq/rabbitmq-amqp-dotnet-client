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
        await publisher.Publish(new AmqpMessage("Hello wold!"),
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
        Assert.Equal("Hello wold!", receivedMessage.Body());
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
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        await management.Queue().Name("ConsumerReQueueMessage").Declare();
        var publisher = connection.PublisherBuilder().Queue("ConsumerReQueueMessage").Build();
        await publisher.Publish(new AmqpMessage("Hello wold!"),
            (_, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });

        TaskCompletionSource<int> tcs = new();
        int consumed = 0;
        IConsumer consumer = connection.ConsumerBuilder().Queue("ConsumerReQueueMessage").MessageHandler(
            (context, message) =>
            {
                Assert.Equal("Hello wold!", message.Body());
                Interlocked.Increment(ref consumed);
                switch (consumed)
                {
                    case 1:
                        // first time requeue the message
                        // it must consumed again
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
}
