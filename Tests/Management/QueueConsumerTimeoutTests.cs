// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
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
}
