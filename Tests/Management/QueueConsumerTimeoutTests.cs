// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Management;

public class QueueConsumerTimeoutTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public void Quorum_builder_sets_x_consumer_timeout_and_quorum_queue_type_in_queue_arguments()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Quorum()
            .ConsumerTimeout(TimeSpan.FromSeconds(90))
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("quorum", args["x-queue-type"]);
        Assert.Equal(90_000L, args["x-consumer-timeout"]);
    }

    [Fact]
    public void Jms_builder_sets_x_consumer_timeout_and_jms_queue_type_in_queue_arguments()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Jms()
            .ConsumerTimeout(TimeSpan.FromMinutes(2))
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("jms", args["x-queue-type"]);
        Assert.Equal(120_000L, args["x-consumer-timeout"]);
    }

    [Fact]
    public void Quorum_consumer_timeout_can_be_combined_with_other_quorum_arguments()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Quorum()
            .DeliveryLimit(7)
            .ConsumerTimeout(TimeSpan.FromMilliseconds(500))
            .QuorumTargetGroupSize(4)
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("quorum", args["x-queue-type"]);
        Assert.Equal(7, args["x-max-delivery-limit"]);
        Assert.Equal(4, args["x-quorum-target-group-size"]);
        Assert.Equal(500L, args["x-consumer-timeout"]);
    }

    [Fact]
    public async Task Declare_quorum_queue_round_trips_x_consumer_timeout_from_broker()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Quorum()
            .ConsumerTimeout(TimeSpan.FromMinutes(15))
            .Queue();

        IQueueInfo declared = await spec.DeclareAsync();
        Assert.Equal(900_000L, declared.Arguments()["x-consumer-timeout"]);

        IQueueInfo fetched = await _management.GetQueueInfoAsync(spec);
        Assert.Equal(900_000L, fetched.Arguments()["x-consumer-timeout"]);
    }

    [SkippableFact]
    public async Task Declare_jms_queue_round_trips_x_consumer_timeout_when_broker_supports_jms()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Jms()
            .ConsumerTimeout(TimeSpan.FromMinutes(10))
            .Queue();

        try
        {
            IQueueInfo declared = await spec.DeclareAsync();
            Assert.Equal(600_000L, declared.Arguments()["x-consumer-timeout"]);
            Assert.Equal("jms", declared.Arguments()["x-queue-type"]);
            Assert.Equal(QueueType.JMS, declared.Type());

            IQueueInfo fetched = await _management.GetQueueInfoAsync(spec);
            Assert.Equal(600_000L, fetched.Arguments()["x-consumer-timeout"]);
        }
        catch (PreconditionFailedException)
        {
            Skip.If(true, "Broker does not support x-queue-type jms (declare returned 409).");
        }
    }

    [Fact]
    public async Task Consumer_with_quorum_attach_consumer_timeout_starts()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue()
            .Name(_queueName)
            .Quorum()
            .ConsumerTimeout(TimeSpan.FromMinutes(10))
            .Queue();

        await queueSpec.DeclareAsync();

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Quorum()
            .ConsumerTimeout(TimeSpan.FromMinutes(2))
            .Builder()
            .MessageHandler((context, message) =>
            {
                context.Accept();
                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task Consumer_builder_quorum_consumer_timeout_throws_with_direct_reply_to()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Quorum().Queue();
        await queueSpec.DeclareAsync();

        NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await _connection.ConsumerBuilder()
                .Queue(queueSpec).Quorum()
                .ConsumerTimeout(TimeSpan.FromMinutes(1)).Builder()
                .SettleStrategy(ConsumerSettleStrategy.DirectReplyTo)
                .MessageHandler((_, _) => Task.CompletedTask)
                .BuildAndStartAsync());

        Assert.Contains("Consumer timeout is not supported", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies that the OnDeliveryRelease callback is invoked when the broker releases a
    /// delivery because the consumer did not settle it within its configured timeout, and
    /// that calling Accept() inside the callback successfully unlocks the consumer.
    /// </summary>
    [Fact]
    public async Task OnDeliveryRelease_callback_is_invoked_when_broker_releases_timed_out_delivery()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        TimeSpan consumerTimeout = TimeSpan.FromSeconds(3);

        IQueueSpecification queueSpec = _management.Queue()
            .Name(_queueName)
            .Quorum()
            .Queue();

        await queueSpec.DeclareAsync();

        TaskCompletionSource<IMessage> releaseTcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Quorum()
            .ConsumerTimeout(consumerTimeout)
            .OnDeliveryRelease((context, message) =>
            {
                context.Accept();
                releaseTcs.TrySetResult(message);
                return Task.CompletedTask;
            })
            .Builder()
            .MessageHandler(async (context, message) =>
            {
                // Hold the message past the consumer timeout so the broker releases it.
                await Task.Delay(TimeSpan.FromSeconds(5));
                try
                {
                    context.Accept();
                }
                catch
                {
                    // Expected: the broker has already released the delivery.
                }
            })
            .BuildAndStartAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();
        try
        {
            await publisher.PublishAsync(new AmqpMessage("timeout-message"));

            // The broker should release the delivery after ~3 s; allow generous headroom.
            IMessage releasedMessage =
                await releaseTcs.Task.WaitAsync(TimeSpan.FromSeconds(15));

            Assert.Equal("timeout-message", releasedMessage.BodyAsString());
        }
        finally
        {
            await publisher.CloseAsync();
            publisher.Dispose();
            await consumer.CloseAsync();
            consumer.Dispose();
        }
    }

    /// <summary>
    /// Verifies that after OnDeliveryRelease fires and Accept() is called (unlocking the consumer),
    /// the consumer can receive and process subsequent messages normally.
    /// </summary>
    [Fact]
    public async Task Consumer_continues_receiving_messages_after_OnDeliveryRelease_accept()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        TimeSpan consumerTimeout = TimeSpan.FromSeconds(3);

        IQueueSpecification queueSpec = _management.Queue()
            .Name(_queueName)
            .Quorum()
            .Queue();

        await queueSpec.DeclareAsync();

        TaskCompletionSource<bool> releaseTcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<IMessage> messageTcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        bool timeoutTriggered = false;

        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Quorum()
            .ConsumerTimeout(consumerTimeout)
            .OnDeliveryRelease((context, _) =>
            {
                context.Accept();
                timeoutTriggered = true;
                releaseTcs.TrySetResult(true);
                return Task.CompletedTask;
            })
            .Builder()
            .MessageHandler(async (context, message) =>
            {
                if (!timeoutTriggered)
                {
                    // First message: delay past timeout to trigger OnDeliveryRelease.
                    await Task.Delay(TimeSpan.FromSeconds(5));
                    try { context.Accept(); } catch { }
                }
                else
                {
                    // Second message: accept immediately after the consumer is unlocked.
                    context.Accept();
                    messageTcs.TrySetResult(message);
                }
            })
            .BuildAndStartAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();
        try
        {
            await publisher.PublishAsync(new AmqpMessage("first-message"));

            // Wait for the timeout/release callback.
            await releaseTcs.Task.WaitAsync(TimeSpan.FromSeconds(15));
            Assert.True(timeoutTriggered);

            // Publish a second message; the consumer should now be unlocked.
            await publisher.PublishAsync(new AmqpMessage("second-message"));

            IMessage m =
                await messageTcs.Task.WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Equal("first-message", m.BodyAsString());
        }
        finally
        {
            await publisher.CloseAsync();
            publisher.Dispose();
            await consumer.CloseAsync();
            consumer.Dispose();
        }
    }
}
