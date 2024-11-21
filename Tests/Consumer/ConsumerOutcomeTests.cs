// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
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
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Utils.ValidateMessageAnnotations(new Dictionary<string, object>
            {
                { wrongAnnotationKey, annotationValue }
            }));

        const string correctAnnotationKey = "x-otp-annotation-key";
        // This should not throw an exception because the annotation key starts with "x-"
        Utils.ValidateMessageAnnotations(new Dictionary<string, object> { { correctAnnotationKey, annotationValue } });
    }

    [Fact]
    public async Task RequeuedMessageShouldBeRequeued()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpecification = _management.Queue(_queueName).Type(QueueType.QUORUM);

        await queueSpecification.DeclareAsync();

        int deliveryCount = 0;
        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();
        var messages = new ConcurrentQueue<IMessage>();
        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder()
            .Queue(queueSpecification)
            .MessageHandler((cxt, msg) =>
            {
                try
                {
                    messages.Enqueue(msg);
                    if (Interlocked.Increment(ref deliveryCount) == 1)
                    {
                        cxt.Requeue();
                    }
                    else
                    {
                        cxt.Accept();
                        tcs.SetResult(true);
                    }
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }

                return Task.CompletedTask;
            });

        IConsumer consumer = await consumerBuilder.BuildAndStartAsync();

        IMessage message = new AmqpMessage($"message");
        IPublisherBuilder publisherBuilder = _connection.PublisherBuilder().Queue(queueSpecification);
        IPublisher publisher = await publisherBuilder.BuildAsync();
        PublishResult pr = await publisher.PublishAsync(message);

        await WhenTcsCompletes(tcs);

        Assert.True(messages.TryDequeue(out IMessage? message0));
        message0.Annotation("x-delivery-count");

        Assert.True(messages.TryDequeue(out IMessage? message1));
        Assert.Equal(1, (long)message1.Annotation("x-delivery-count"));

        await WaitUntilStable(async () =>
        {
            IQueueInfo qi = await _management.GetQueueInfoAsync(queueSpecification);
            return (int)qi.MessageCount();
        }, 0);

        await consumer.CloseAsync();
        consumer.Dispose();
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

        const string annotationKey = "x-opt-annotation-key";
        const string annotationValue = "annotation-value";

        const string annotationKey1 = "x-opt-annotation1-key";
        const string annotationValue1 = "annotation1-value";

        int requeueCount = 0;

        await _management.Queue().Type(QueueType.QUORUM).Name(_queueName).DeclareAsync();

        TaskCompletionSource<bool> tcsRequeue = CreateTaskCompletionSource();
        List<IMessage> messages = [];
        IPublisher publisher = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();
        IConsumer consumer = await _connection.ConsumerBuilder().MessageHandler(
            (context, message) =>
            {
                try
                {
                    messages.Add(message);
                    if (Interlocked.Increment(ref requeueCount) == 1)
                    {
                        context.Requeue(new Dictionary<string, object>
                        {
                            { annotationKey, annotationValue },
                            { annotationKey1, annotationValue1 }
                        });
                    }
                    else
                    {
                        context.Accept();
                        tcsRequeue.SetResult(true);
                    }
                }
                catch (Exception ex)
                {
                    tcsRequeue.SetException(ex);
                }

                return Task.CompletedTask;
            }
        ).Queue(_queueName).BuildAndStartAsync();

        IMessage message = new AmqpMessage(RandomString());
        PublishResult pr = await publisher.PublishAsync(message);

        Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

        await WhenTcsCompletes(tcsRequeue);

        Assert.Equal(2, messages.Count);
        Assert.Null(messages[0].Annotation(annotationKey));
        Assert.Null(messages[0].Annotation(annotationKey1));
        Assert.Null(messages[0].Annotation("x-delivery-count"));

        Assert.Equal(messages[1].Annotation(annotationKey), annotationValue);
        Assert.Equal(messages[1].Annotation(annotationKey1), annotationValue1);
        Assert.NotNull(messages[1].Annotation("x-delivery-count"));

        using HttpApiClient client = new();
        Queue q = await client.GetQueueAsync(_queueName);
        Assert.Equal(0, q.Messages);

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task DiscardedMessageShouldBeDeadLeadLetteredWhenConfigured()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        string dlqQueueName = $"dlq_{_queueName}";
        await DeclareDeadLetterTopology(_queueName, dlqQueueName);

        IConsumerBuilder queueConsumerBuilder = _connection.ConsumerBuilder()
            .Queue(_queueName)
            .MessageHandler((cxt, msg) =>
            {
                cxt.Discard();
                return Task.CompletedTask;
            });
        IConsumer queueConsumer = await queueConsumerBuilder.BuildAndStartAsync();

        TaskCompletionSource<IMessage> tcs = CreateTaskCompletionSource<IMessage>();
        IConsumerBuilder deadLetterQueueConsumerBuilder = _connection.ConsumerBuilder()
            .Queue(dlqQueueName)
            .MessageHandler((cxt, msg) =>
            {
                try
                {
                    cxt.Accept();
                    tcs.SetResult(msg);
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }

                return Task.CompletedTask;
            });

        IConsumer deadLetterConsumer = await deadLetterQueueConsumerBuilder.BuildAndStartAsync();

        Guid messageId = Guid.NewGuid();
        IMessage message = new AmqpMessage(RandomString())
            .MessageId(messageId);

        IPublisherBuilder publisherBuilder = _connection.PublisherBuilder()
            .Queue(_queueName);
        IPublisher publisher = await publisherBuilder.BuildAsync();

        PublishResult pr = await publisher.PublishAsync(message);
        await publisher.CloseAsync();
        publisher.Dispose();

        IMessage deadLetteredMessage = await WhenTcsCompletes(tcs);
        Assert.Equal(messageId, deadLetteredMessage.MessageId());

        await WaitUntilStable(async () =>
        {
            IQueueInfo qi = await _management.GetQueueInfoAsync(_queueName);
            return (int)qi.MessageCount();
        }, 0);

        await WaitUntilStable(async () =>
        {
            IQueueInfo qi = await _management.GetQueueInfoAsync(dlqQueueName);
            return (int)qi.MessageCount();
        }, 0);

        await queueConsumer.CloseAsync();
        queueConsumer.Dispose();

        await deadLetterConsumer.CloseAsync();
        deadLetterConsumer.Dispose();
    }

    [Fact]
    public async Task DiscardedMessageWithAnnotationsShouldBeDeadLeadLetteredAndContainAnnotationsWhenConfigured()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        string dlqQueueName = $"dlq_{_queueName}";
        await DeclareDeadLetterTopology(_queueName, dlqQueueName);

        const string annotationKey = "x-opt-annotation-key";
        const string annotationValue = "annotation-value";
        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();
        IPublisher publisher = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .MessageHandler((context, _) =>
            {
                context.Discard(new Dictionary<string, object> { { annotationKey, annotationValue } });
                tcs.SetResult(true);
                return Task.CompletedTask;
            }
        ).Queue(_queueName).BuildAndStartAsync();

        IMessage message = new AmqpMessage(RandomString());
        PublishResult pr = await publisher.PublishAsync(message);
        Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

        await WhenTcsCompletes(tcs);

        await consumer.CloseAsync();
        consumer.Dispose();

        TaskCompletionSource<IMessage> tcsDl = CreateTaskCompletionSource<IMessage>();
        IConsumer dlConsumer = await _connection.ConsumerBuilder()
            .MessageHandler((context, message1) =>
            {
                context.Accept();
                tcsDl.SetResult(message1);
                return Task.CompletedTask;
            })
            .Queue(dlqQueueName).BuildAndStartAsync();

        IMessage mResult = await WhenTcsCompletes(tcsDl);
        Assert.NotNull(mResult);
        Assert.Equal(mResult.Annotation(annotationKey), annotationValue);

        using HttpApiClient client = new();
        Queue q = await client.GetQueueAsync(_queueName);
        Assert.Equal(0, q.Messages);

        Queue q1 = await client.GetQueueAsync(dlqQueueName);
        Assert.Equal(0, q1.Messages);

        await dlConsumer.CloseAsync();
        dlConsumer.Dispose();
    }

    private async Task DeclareDeadLetterTopology(string queueName, string dlxQueueName)
    {
        Assert.NotNull(_management);

        string dlx = $"{queueName}.dlx";
        await _management.Queue().Name(queueName).Type(QueueType.QUORUM).DeadLetterExchange(dlx).DeclareAsync();
        await _management.Exchange(dlx).Type(ExchangeType.FANOUT).AutoDelete(true).DeclareAsync();
        await _management.Queue(dlxQueueName).Exclusive(true).DeclareAsync();
        await _management.Binding().SourceExchange(dlx).DestinationQueue(dlxQueueName).BindAsync();
    }
}
