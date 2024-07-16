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
            (message, descriptor) => { Assert.Equal(OutcomeState.Accepted, descriptor.State); });

        TaskCompletionSource<IMessage> tcs = new();
        IConsumer consumer = connection.ConsumerBuilder().Queue("SimpleConsumeMessage").MessageHandler(
            (context, message) =>
            {
                tcs.SetResult(message);
            }
        ).Build();
        await Task.WhenAny(tcs.Task, Task.Delay(5000));
        Assert.True(tcs.Task.IsCompleted);
        IMessage receivedMessage = await tcs.Task;
        Assert.Equal("Hello wold!", receivedMessage.Body());
        await management.QueueDeletion().Delete("SimpleConsumeMessage");
        await connection.CloseAsync();
    }


}
