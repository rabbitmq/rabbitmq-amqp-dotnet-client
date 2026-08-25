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

public class QueueDelayedTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public void Delayed_builder_sets_x_queue_type_delayed_in_queue_arguments()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Delayed()
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("delayed", args["x-queue-type"]);
    }

    [Fact]
    public void Delayed_builder_sets_delivery_limit_and_quorum_group_sizes()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Delayed()
            .DeliveryLimit(7)
            .QuorumInitialGroupSize(3)
            .QuorumTargetGroupSize(5)
            .DeadLetterStrategy(QuorumQueueDeadLetterStrategy.AtLeastOnce)
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("delayed", args["x-queue-type"]);
        Assert.Equal(7, args["x-delivery-limit"]);
        Assert.Equal(3, args["x-quorum-initial-group-size"]);
        Assert.Equal(5, args["x-quorum-target-group-size"]);
        Assert.Equal("at-least-once", args["x-dead-letter-strategy"]);
    }

    [Fact]
    public void Delayed_builder_can_be_combined_with_common_queue_arguments()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Delayed()
            .Queue()
            .DeadLetterExchange("my-dlx")
            .MessageTtl(TimeSpan.FromSeconds(30))
            .MaxLength(1000);

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("delayed", args["x-queue-type"]);
        Assert.Equal("my-dlx", args["x-dead-letter-exchange"]);
        Assert.Equal(30_000L, args["x-message-ttl"]);
        Assert.Equal(1000L, args["x-max-length"]);
    }

    [Fact]
    public void Delayed_builder_shovel_destination_applies_default_shovel_arguments()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Delayed()
            .ShovelDestination("downstream-queue")
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("downstream-queue", args["x-shovel-destination"]);
        Assert.Equal("amqp://", args["x-shovel-destination-uri"]);
        Assert.Equal("amqp091", args["x-shovel-protocol"]);
        Assert.Equal(50, args["x-shovel-prefetch-count"]);
        Assert.Equal("on-confirm", args["x-shovel-ack-mode"]);
    }

    [Fact]
    public void Delayed_builder_shovel_settings_can_override_defaults()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Delayed()
            .ShovelDestination("downstream-queue")
            .ShovelDestinationRoutingKey("routing-key")
            .ShovelDestinationUri("amqp://guest:guest@localhost:5672")
            .ShovelProtocol("amqp10")
            .ShovelPrefetch(200)
            .ShovelAcknowledgement("no-ack")
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("downstream-queue", args["x-shovel-destination"]);
        Assert.Equal("routing-key", args["x-shovel-destination-key"]);
        Assert.Equal("amqp://guest:guest@localhost:5672", args["x-shovel-destination-uri"]);
        Assert.Equal("amqp10", args["x-shovel-protocol"]);
        Assert.Equal(200, args["x-shovel-prefetch-count"]);
        Assert.Equal("no-ack", args["x-shovel-ack-mode"]);
    }

    [SkippableFact]
    public async Task Declare_delayed_queue_round_trips_x_queue_type_when_broker_supports_delayed()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Delayed()
            .DeliveryLimit(5)
            .Queue();

        try
        {
            IQueueInfo declared = await spec.DeclareAsync();
            Assert.Equal("delayed", declared.Arguments()["x-queue-type"]);
            Assert.Equal(QueueType.DELAYED, declared.Type());

            IQueueInfo fetched = await _management.GetQueueInfoAsync(spec);
            Assert.Equal("delayed", fetched.Arguments()["x-queue-type"]);
        }
        catch (PreconditionFailedException)
        {
            Skip.If(true, "Broker does not support x-queue-type delayed (declare returned 409).");
        }
    }
}
