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
        var q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();
        await PublishAsync(q, 10);

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

    /// <summary>
    /// This is a standard case for the stream consumer with SubscriptionListener
    /// The consumer should start from the offset 5 and consume 5 messages
    /// Since: ctx.StreamOptions.Offset(5)
    /// </summary>
    [Fact]
    public async Task StreamConsumerBuilderShouldStartFromTheListenerConfiguration()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        ManualResetEventSlim manualResetEvent = new(false);
        var q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();
        await PublishAsync(q, 10);
        int totalConsumed = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(_queueName).Stream().FilterMatchUnfiltered(true).Offset(StreamOffsetSpecification.First).Builder()
            .InitialCredits(10).MessageHandler(
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
                ctx => { ctx.StreamOptions.Offset(5); }
            ).BuildAndStartAsync();

        manualResetEvent.Wait(TimeSpan.FromSeconds(5));
        Assert.Equal(5, totalConsumed);
        await consumer.CloseAsync();
    }

    /// <summary>
    /// In this test we simulate a listener that changes the offset after the connection is killed
    /// We simulate this by changing the offset from 4 to 6 like loading the offset from an external storage
    /// Each time the consumer is created the listener will change the offset and must be called to set the new offset
    /// </summary>
    [Fact]
    public async Task StreamConsumerBuilderShouldStartFromTheListenerConfigurationWhenConnectionIsKilled()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        ManualResetEventSlim manualResetEvent = new(false);
        var q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();
        await PublishAsync(q, 10);
        int totalConsumed = 0;
        int startFrom = 2;
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
            ).Stream()
            .Offset(StreamOffsetSpecification
                .First) // in this case this value is ignored because of the listener will replace it
            .Builder().SubscriptionListener(
                ctx =>
                {
                    // Here we simulate a listener that changes the offset after the connection is killed
                    // Like loading the offset from an external storage
                    // so first start from 4 then start from 6
                    // Given 10 messages, we should consume 6 messages first time 
                    // then 4 messages the second time the total should be 10
                    startFrom += 2;
                    ctx.StreamOptions.Offset(startFrom);
                    // In this case we should not be able to call the builder
                    Assert.Throws<NotImplementedException>(() => ctx.StreamOptions.Builder());
                }
            ).BuildAndStartAsync();

        manualResetEvent.Wait(TimeSpan.FromSeconds(5));
        Assert.Equal(6, totalConsumed);
        manualResetEvent.Reset();
        await SystemUtils.WaitUntilConnectionIsKilled(_containerId);
        manualResetEvent.Wait(TimeSpan.FromSeconds(5));
        Assert.Equal(10, totalConsumed);
        await consumer.CloseAsync();
    }
}
