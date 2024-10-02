using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EasyNetQ.Management.Client.Model;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;
using PublishResult = RabbitMQ.AMQP.Client.PublishResult;
using QueueType = RabbitMQ.AMQP.Client.QueueType;

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
        Utils.ValidateMessageAnnotations(new Dictionary<string, object> { { correctAnnotationKey, annotationValue } });
    }

    /// <summary>
    /// The test verifies that a requeued message with annotations will contain the annotations on redelivery.
    /// The delivered message should contain the custom annotations and x-delivery-count
    /// </summary>
    [Fact]
    public async Task RequeuedMessageWithAnnotationShouldContainAnnotationsOnRedelivery()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        TaskCompletionSource<bool> tcsRequeue =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        const string annotationKey = "x-opt-annotation-key";
        const string annotationValue = "annotation-value";

        const string annotationKey1 = "x-opt-annotation1-key";
        const string annotationValue1 = "annotation1-value";

        int requeueCount = 0;

        await _management.Queue().Type(QueueType.QUORUM).Name(_queueName).DeclareAsync();
        List<IMessage> messages = [];
        IPublisher publisher = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();
        IConsumer consumer = await _connection.ConsumerBuilder().MessageHandler(
            async (context, message) =>
            {
                messages.Add(message);
                if (requeueCount == 0)
                {
                    requeueCount++;
                    await context.RequeueAsync(new Dictionary<string, object>
                    {
                        { annotationKey, annotationValue }, { annotationKey1, annotationValue1 }
                    });
                }
                else
                {
                    await context.AcceptAsync();
                    tcsRequeue.SetResult(true);
                }
            }
        ).Queue(_queueName).BuildAndStartAsync();

        IMessage message = new AmqpMessage($"message");
        PublishResult pr = await publisher.PublishAsync(message);

        Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

        await tcsRequeue.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(2, messages.Count);
        Assert.Null(messages[0].Annotation(annotationKey));
        Assert.Null(messages[0].Annotation(annotationKey1));
        Assert.Null(messages[0].Annotation("x-delivery-count"));

        Assert.Equal(messages[1].Annotation(annotationKey), annotationValue);
        Assert.Equal(messages[1].Annotation(annotationKey1), annotationValue1);
        Assert.NotNull(messages[1].Annotation("x-delivery-count"));
        HttpApiClient client = new();
        Queue q = await client.GetQueueAsync(_queueName);
        Assert.Equal(0, q.Messages);

        await consumer.CloseAsync();
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

        var client = new HttpApiClient();
        Queue q = await client.GetQueueAsync(_queueName);
        Assert.Equal(0, q.Messages);

        Queue q1 = await client.GetQueueAsync(dlqQueueName);
        Assert.Equal(0, q1.Messages);

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
