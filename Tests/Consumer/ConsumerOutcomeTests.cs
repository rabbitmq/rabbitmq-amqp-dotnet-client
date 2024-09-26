using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

public class ConsumerOutcomeTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public void ValidateAnnotations()
    {
        const string wrongAnnotationKey = "missing-the-start-x-annotation-key";
        const string annotationValue = "annotation-value";
        // This should throw an exception because the annotation key does not start with "x-"
        Assert.Throws<ArgumentException>(() =>
            Utils.ValidateMessageAnnotations(new Dictionary<string, object>
            {
                { wrongAnnotationKey, annotationValue }
            }));

        const string correctAnnotationKey = "x-otp-annotation-key";
        // This should not throw an exception because the annotation key starts with "x-"
        Utils.ValidateMessageAnnotations(new Dictionary<string, object>
        {
            { correctAnnotationKey, annotationValue }
        });
    }

    [Fact]
    public async Task DiscardedMessageWithAnnotationsShouldBeDeadLeadLetteredAndContainAnnotationsWhenConfigured()
    {
        string dlqQueueName = $"dlq_{_queueName}";
        await DeclareDeadLetterTopology(_queueName, dlqQueueName);


        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const string annotationKey = "x-opt-annotation-key";
        const string annotationValue = "annotation-value";
        TaskCompletionSource<bool> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        IPublisher publisher = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();
        IConsumer consumer = await _connection.ConsumerBuilder().MessageHandler(
            async (context, _) =>
            {
                await context.DiscardAsync(new Dictionary<string, object> { { annotationKey, annotationValue } });
                tcs.SetResult(true);
            }
        ).Queue(_queueName).BuildAndStartAsync();

        IMessage message = new AmqpMessage($"message");
        PublishResult pr = await publisher.PublishAsync(message);
        Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await consumer.CloseAsync();
        TaskCompletionSource<IMessage> tcsDl =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        IConsumer dlConsumer = await _connection.ConsumerBuilder().MessageHandler(async (context, message1) =>
        {
            await context.AcceptAsync();
            tcsDl.SetResult(message1);
        }).Queue(dlqQueueName).BuildAndStartAsync();

        IMessage mResult = await tcsDl.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(mResult);
        Assert.Equal(mResult.Annotation(annotationKey), annotationValue);
        await dlConsumer.CloseAsync();
    }


    private async Task DeclareDeadLetterTopology(string queueName, string dlxQueueName)
    {
        string dlx = $"{queueName}.dlx";
        Assert.NotNull(_management);
        await _management.Queue().Name(queueName).Type(QueueType.QUORUM).DeadLetterExchange(dlx).DeclareAsync();
        await _management.Exchange(dlx).Type(ExchangeType.FANOUT).AutoDelete(true).DeclareAsync();
        await _management.Queue(dlxQueueName).Exclusive(true).DeclareAsync();
        await _management.Binding().SourceExchange(dlx).DestinationQueue(dlxQueueName).BindAsync();
    }
}
