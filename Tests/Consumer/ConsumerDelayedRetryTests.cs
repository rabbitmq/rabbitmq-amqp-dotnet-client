// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

/// <summary>
/// Integration tests and IContext.DelayedRetry(TimeSpan,true).
///
/// Both methods send the AMQP 1.0 modified{delivery-failed=true, undeliverable-here=false}
/// outcome. This increments the broker-side delivery-count, causing the message to be
/// redelivered. When the queue is configured with x-delayed-retry-type=failed the broker
/// also applies a linear back-off delay before redelivery (RabbitMQ 4.3+).
///
/// Note: QuorumQueueDelayedRetryType.Failed queue support is planned for a future release.
/// These tests verify the core AMQP disposition on a plain quorum queue.
/// </summary>
public class ConsumerDelayedRetryTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    /// <summary>
    /// Verifies that context.DelayedRetry() causes the message to be redelivered
    /// with an incremented x-acquired-count.
    /// </summary>
    [SkippableFact]
    public async Task DelayedRetryMessageShouldBeRedelivered()
    {
        Assert.NotNull(_featureFlags);
        Skip.IfNot(_featureFlags is { Is43OrMore: true }, "At least RabbitMQ 4.3.0 required");

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue(_queueName).Type(QueueType.QUORUM);
        await queueSpec.DeclareAsync();

        int recvs = 0;
        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();
        List<IMessage> received = [];

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .MessageHandler((context, message) =>
            {
                try
                {
                    received.Add(message);
                    if (Interlocked.Increment(ref recvs) == 1)
                    {
                        context.DelayedRetry(TimeSpan.FromSeconds(1));
                    }
                    else
                    {
                        context.Accept();
                        tcs.SetResult(true);
                    }
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();
        await publisher.PublishAsync(new AmqpMessage("delayed-retry-test"));

        await WhenTcsCompletes(tcs);

        Assert.Equal(2, received.Count);
        Assert.Equal(1, (long)received[1].Annotation("x-acquired-count"));

        await WaitUntilQueueMessageCount(queueSpec, 0);
        await consumer.CloseAsync();
        consumer.Dispose();
        await publisher.CloseAsync();
        publisher.Dispose();
    }

    /// <summary>
    /// Verifies that context.DelayedRetry(TimeSpan) causes the message to be redelivered.
    /// The x-opt-delivery-time annotation is set so the broker waits at least `delay`
    /// before redelivering. x-acquired-count is incremented on redelivery.
    /// </summary>
    [SkippableFact]
    public async Task DelayedRetryWithDelayShouldBeRedelivered()
    {
        Assert.NotNull(_featureFlags);
        Skip.IfNot(_featureFlags is { Is43OrMore: true }, "At least RabbitMQ 4.3.0 required");

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue(_queueName).Type(QueueType.QUORUM);
        await queueSpec.DeclareAsync();

        int deliveryCount = 0;
        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();
        List<IMessage> received = [];

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .MessageHandler((context, message) =>
            {
                try
                {
                    received.Add(message);
                    if (Interlocked.Increment(ref deliveryCount) == 1)
                    {
                        context.DelayedRetry(TimeSpan.FromSeconds(1));
                    }
                    else
                    {
                        context.Accept();
                        tcs.SetResult(true);
                    }
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();
        await publisher.PublishAsync(new AmqpMessage("delayed-retry-with-delay-test"));

        await WhenTcsCompletes(tcs);

        Assert.Equal(2, received.Count);
        Assert.Equal(1, (long)received[1].Annotation("x-acquired-count"));

        await WaitUntilQueueMessageCount(queueSpec, 0);
        await consumer.CloseAsync();
        consumer.Dispose();
        await publisher.CloseAsync();
        publisher.Dispose();
    }

    /// <summary>
    /// Verifies that after repeatedly calling DelayedRetry() up to the delivery limit,
    /// the message is dead-lettered.
    /// </summary>
    [SkippableFact]
    public async Task DelayedRetryMessageShouldRespectDeliveryLimit()
    {
        Assert.NotNull(_featureFlags);
        Skip.IfNot(_featureFlags is { Is43OrMore: true }, "At least RabbitMQ 4.3.0 required");

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        string dlqName = $"dlq-{_queueName}";
        IQueueSpecification queueSpec = _management.Queue(_queueName)
            .Type(QueueType.QUORUM)
            .Quorum()
                .DeliveryLimit(2)
            .Queue()
            .DeadLetterExchange("amq.fanout");

        IQueueSpecification dlqSpec = _management.Queue(dlqName).Exclusive(true);

        await queueSpec.DeclareAsync();
        await dlqSpec.DeclareAsync();
        await _management.Binding()
            .SourceExchange("amq.fanout")
            .DestinationQueue(dlqName)
            .BindAsync();

        TaskCompletionSource<IMessage> dlqTcs = CreateTaskCompletionSource<IMessage>();

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .MessageHandler((context, _) =>
            {
                context.DelayedRetry(TimeSpan.FromMilliseconds(200), true);
                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        IConsumer dlqConsumer = await _connection.ConsumerBuilder()
            .Queue(dlqName)
            .MessageHandler((context, message) =>
            {
                try
                {
                    context.Accept();
                    dlqTcs.TrySetResult(message);
                }
                catch (Exception ex)
                {
                    dlqTcs.TrySetException(ex);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();
        await publisher.PublishAsync(new AmqpMessage("delivery-limit-test"));

        IMessage deadLetteredMsg = await WhenTcsCompletes(dlqTcs);
        Assert.NotNull(deadLetteredMsg);

        await WaitUntilQueueMessageCount(queueSpec, 0);
        await WaitUntilQueueMessageCount(dlqName, 0);

        await consumer.CloseAsync();
        consumer.Dispose();
        await dlqConsumer.CloseAsync();
        dlqConsumer.Dispose();
        await publisher.CloseAsync();
        publisher.Dispose();
        await dlqSpec.DeleteAsync();
    }

    /// <summary>
    /// Verifies that calling DelayedRetry() on a pre-settled delivery context
    /// throws an InvalidOperationException.
    /// </summary>
    [Fact]
    public async Task PreSettledConsumerDelayedRetryThrowsException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue(_queueName);
        await queueSpec.DeclareAsync();
        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<InvalidOperationException> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .SettleStrategy(ConsumerSettleStrategy.PreSettled)
            .MessageHandler((context, _) =>
            {
                try
                {
                    context.DelayedRetry(TimeSpan.FromMilliseconds(200), true);
                }
                catch (InvalidOperationException ex)
                {
                    tcs.SetResult(ex);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await WhenTcsCompletes(tcs);
        InvalidOperationException exception = await tcs.Task;
        Assert.Contains("Cannot delayed-retry a pre-settled delivery context", exception.Message);

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    /// <summary>
    /// Verifies that calling DelayedRetry(TimeSpan) on a pre-settled delivery context
    /// throws an InvalidOperationException.
    /// </summary>
    [Fact]
    public async Task PreSettledConsumerDelayedRetryWithDelayThrowsException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue(_queueName);
        await queueSpec.DeclareAsync();
        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<InvalidOperationException> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .SettleStrategy(ConsumerSettleStrategy.PreSettled)
            .MessageHandler((context, _) =>
            {
                try
                {
                    context.DelayedRetry(TimeSpan.FromSeconds(1));
                }
                catch (InvalidOperationException ex)
                {
                    tcs.SetResult(ex);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await WhenTcsCompletes(tcs);
        InvalidOperationException exception = await tcs.Task;
        Assert.Contains("Cannot delayed-retry a pre-settled delivery context", exception.Message);

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    /// <summary>
    /// Verifies that BatchDeliveryContext.DelayedRetry() sends the delivery-failed
    /// disposition for all accumulated messages, causing each to be redelivered.
    /// </summary>
    [SkippableFact]
    public async Task BatchDelayedRetryShouldRequeueAllMessages()
    {
        Assert.NotNull(_featureFlags);
        Skip.IfNot(_featureFlags is { Is43OrMore: true }, "At least RabbitMQ 4.3.0 required");

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue(_queueName).Type(QueueType.QUORUM);
        await queueSpec.DeclareAsync();

        const int batchSize = 5;
        await PublishAsync(queueSpec, batchSize);

        BatchDeliveryContext batch = new();
        bool batchFired = false;
        int totalDeliveries = 0;
        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .MessageHandler((context, _) =>
            {
                try
                {
                    int count = Interlocked.Increment(ref totalDeliveries);

                    if (!batchFired)
                    {
                        batch.Add(context);
                        if (batch.Count() == batchSize)
                        {
                            batchFired = true;
                            batch.DelayedRetry(TimeSpan.FromMilliseconds(200), true);
                        }
                    }
                    else
                    {
                        context.Accept();
                        if (count == batchSize * 2)
                        {
                            tcs.SetResult(true);
                        }
                    }
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await WhenTcsCompletes(tcs);

        await WaitUntilQueueMessageCount(queueSpec, 0);
        await consumer.CloseAsync();
        consumer.Dispose();
    }
}
