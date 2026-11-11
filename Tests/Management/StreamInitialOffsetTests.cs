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

public class StreamInitialOffsetTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public void Stream_builder_sets_x_stream_initial_offset_and_stream_queue_type_in_queue_arguments()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Stream()
            .InitialOffset(1000)
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("stream", args["x-queue-type"]);
        Assert.Equal(1000L, args["x-stream-initial-offset"]);
    }

    [Fact]
    public void Stream_initial_offset_zero_is_sent()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue().Name(_queueName).Stream().InitialOffset(0).Queue();

        Assert.Equal(0L, spec.QueueArguments["x-stream-initial-offset"]);
    }

    [Fact]
    public void Stream_initial_offset_accepts_max_value()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue().Name(_queueName).Stream()
            .InitialOffset(Consts.MaxStreamInitialOffset).Queue();

        Assert.Equal((1L << 62) - 1, spec.QueueArguments["x-stream-initial-offset"]);
    }

    [Fact]
    public void Stream_initial_offset_not_set_by_default()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue().Name(_queueName).Stream().Queue();

        Assert.False(spec.QueueArguments.ContainsKey("x-stream-initial-offset"));
    }

    [Fact]
    public void Stream_initial_offset_can_be_combined_with_other_stream_arguments()
    {
        Assert.NotNull(_management);

        IQueueSpecification spec = _management.Queue()
            .Name(_queueName)
            .Stream()
            .MaxAge(TimeSpan.FromHours(1))
            .InitialOffset(42)
            .InitialClusterSize(1)
            .Queue();

        Dictionary<object, object> args = spec.QueueArguments;
        Assert.Equal("stream", args["x-queue-type"]);
        Assert.Equal(42L, args["x-stream-initial-offset"]);
        Assert.Equal(1, args["x-initial-cluster-size"]);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(long.MinValue)]
    [InlineData(Consts.MaxStreamInitialOffset + 1)]
    [InlineData(long.MaxValue)]
    public void Stream_initial_offset_out_of_range_throws(long offset)
    {
        Assert.NotNull(_management);

        Assert.Throws<ArgumentException>(() =>
            _management.Queue().Name(_queueName).Stream().InitialOffset(offset));
    }

    [Fact]
    public async Task Declare_throws_not_supported_when_broker_lacks_stream_initial_offset()
    {
        Assert.NotNull(_management);
        Assert.NotNull(_featureFlags);

        IQueueSpecification spec = _management.Queue().Name(_queueName).Stream().InitialOffset(1000).Queue();

        _featureFlags.IsStreamInitialOffsetSupported = false;

        NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(() => spec.DeclareAsync());
        Assert.Contains("4.4", ex.Message);
    }

    [Fact]
    public async Task Declare_without_initial_offset_works_when_flag_is_false()
    {
        Assert.NotNull(_management);
        Assert.NotNull(_featureFlags);

        _featureFlags.IsStreamInitialOffsetSupported = false;

        IQueueInfo info = await _management.Queue().Name(_queueName).Stream().Queue().DeclareAsync();
        Assert.Equal(QueueType.STREAM, info.Type());
    }

    [SkippableFact]
    public async Task Declare_stream_round_trips_x_stream_initial_offset_from_broker()
    {
        Assert.NotNull(_management);
        Skip.IfNot(_featureFlags is { Is44OrMore: true }, "At least RabbitMQ 4.4.0 required");

        IQueueSpecification spec = _management.Queue().Name(_queueName).Stream().InitialOffset(1000).Queue();

        IQueueInfo declared = await spec.DeclareAsync();
        Assert.Equal(1000L, declared.Arguments()["x-stream-initial-offset"]);

        IQueueInfo fetched = await _management.GetQueueInfoAsync(spec);
        Assert.Equal(1000L, fetched.Arguments()["x-stream-initial-offset"]);
    }

    [SkippableFact]
    public async Task Consumed_messages_start_from_stream_initial_offset()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        Skip.IfNot(_featureFlags is { Is44OrMore: true }, "At least RabbitMQ 4.4.0 required");

        const long initialOffset = 1000;
        const int messageCount = 5;

        IQueueSpecification spec = _management.Queue().Name(_queueName).Stream()
            .InitialOffset(initialOffset).Queue();
        await spec.DeclareAsync();
        await WaitForQueueReplicas(_queueName);

        await PublishAsync(spec, messageCount);

        List<long> offsets = new();
        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();

        using IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(_queueName)
            .Stream()
            .Offset(StreamOffsetSpecification.First)
            .Builder()
            .MessageHandler((ctx, msg) =>
            {
                lock (offsets)
                {
                    offsets.Add((long)msg.Annotation("x-stream-offset"));
                    if (offsets.Count == messageCount)
                    {
                        tcs.TrySetResult(true);
                    }
                }

                ctx.Accept();
                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await WhenTcsCompletes(tcs);

        lock (offsets)
        {
            Assert.Equal(new List<long> { 1000, 1001, 1002, 1003, 1004 }, offsets);
        }
    }
}
