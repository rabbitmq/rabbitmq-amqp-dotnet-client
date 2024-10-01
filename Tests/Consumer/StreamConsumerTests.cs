using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

/// <summary>
/// These tests are only for the streaming part of the consumer.
/// we'd need to test the consumer itself in a different use cases
/// like restarting from an offset with and without the subscription listener
/// </summary>
public class StreamConsumerTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{

    /// <summary>
    /// Given a stream, if the connection is killed the consumer will restart consuming from the beginning
    /// because of Offset(StreamOffsetSpecification.First)
    /// so the total number of consumed messages should be 10 two times = 20
    /// </summary>
    [Fact]
    public async Task StreamConsumerBuilderShouldRestartFromTheBeginning()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        ManualResetEventSlim manualResetEvent = new(false);
        await _management.Queue(_queueName).Stream().Queue().DeclareAsync();

        await PublishMessages();

        int totalConsumed = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(_queueName).InitialCredits(10).MessageHandler(
                async (context, message) =>
                {
                    Interlocked.Increment(ref totalConsumed);
                    await context.AcceptAsync();
                    if (message.MessageId() == "9")
                    {
                        manualResetEvent.Set();
                    }
                }
            ).Stream().Offset(StreamOffsetSpecification.First).Builder().BuildAndStartAsync();

        manualResetEvent.Wait(TimeSpan.FromSeconds(5));
        manualResetEvent.Reset();
        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);
        manualResetEvent.Wait(TimeSpan.FromSeconds(5));
        Assert.Equal(20, totalConsumed);
        await consumer.CloseAsync();
    }



    [Fact]
    public async Task StreamConsumerBuilderShouldStartFromTheListenerConfiguration()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        ManualResetEventSlim manualResetEvent = new(false);
        await _management.Queue(_queueName).Stream().Queue().DeclareAsync();

        await PublishMessages();

        int totalConsumed = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(_queueName).InitialCredits(10).MessageHandler(
                async (context, message) =>
                {
                    Interlocked.Increment(ref totalConsumed);
                    await context.AcceptAsync();
                    if (message.MessageId() == "9")
                    {
                        manualResetEvent.Set();
                    }
                }
            ).Stream().Builder().SubscriptionListener(
                ctx =>
                {
                    ctx.StreamOptions.Offset(5);
                }
                ).BuildAndStartAsync();

        manualResetEvent.Wait(TimeSpan.FromSeconds(5));
        Assert.Equal(5, totalConsumed);
        await consumer.CloseAsync();
    }

    private async Task PublishMessages()
    {
        Assert.NotNull(_connection);
        IPublisher publisher = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();
        for (int i = 0; i < 10; i++)
        {
            PublishResult pr = await publisher.PublishAsync(new AmqpMessage($"Hello World_{i}").MessageId(i.ToString()));
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
        }
    }
}
