// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

public class PreSettledConsumerTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task PreSettledConsumerCanReceiveMessages()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<IMessage> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                // With PreSettled, messages are already settled, so we don't need to call Accept()
                tcs.SetResult(message);
                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await WhenTcsCompletes(tcs);
        IMessage receivedMessage = await tcs.Task;
        Assert.Equal("message_0", receivedMessage.BodyAsString());

        // UnsettledMessageCount should be 0 for pre-settled consumers
        Assert.Equal(0, consumer.UnsettledMessageCount);

        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal((ulong)0, queueInfo.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task PreSettledConsumerAcceptThrowsException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<InvalidOperationException> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                try
                {
                    context.Accept();
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
        Assert.Contains("Cannot accept a pre-settled delivery context", exception.Message);
        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal((ulong)0, queueInfo.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task PreSettledConsumerDiscardThrowsException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<InvalidOperationException> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                try
                {
                    context.Discard();
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
        Assert.Contains("Cannot discard a pre-settled delivery context", exception.Message);
        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal((ulong)0, queueInfo.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task PreSettledConsumerDiscardWithAnnotationsThrowsException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<InvalidOperationException> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                try
                {
                    var annotations = new Dictionary<string, object> { { "x-reason", "test" } };
                    context.Discard(annotations);
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
        Assert.Contains("Cannot discard a pre-settled delivery context", exception.Message);
        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal((ulong)0, queueInfo.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task PreSettledConsumerRequeueThrowsException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<InvalidOperationException> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                try
                {
                    context.Requeue();
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
        Assert.Contains("Cannot requeue a pre-settled delivery context", exception.Message);
        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal((ulong)0, queueInfo.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task PreSettledConsumerRequeueWithAnnotationsThrowsException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<InvalidOperationException> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                try
                {
                    var annotations = new Dictionary<string, object> { { "x-reason", "test" } };
                    context.Requeue(annotations);
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
        Assert.Contains("Cannot requeue a pre-settled delivery context", exception.Message);
        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal((ulong)0, queueInfo.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task PreSettledConsumerBatchThrowsException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<InvalidOperationException> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                try
                {
                    context.Batch();
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
        Assert.Contains("Cannot create a batch context from a pre-settled delivery context", exception.Message);
        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal((ulong)0, queueInfo.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task PreSettledConsumerUnsettledMessageCountIsZero()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        const int messageCount = 5;
        await PublishAsync(queueSpec, messageCount);

        TaskCompletionSource<bool> tcs = new();
        int receivedCount = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                receivedCount++;
                if (receivedCount == messageCount)
                {
                    tcs.SetResult(true);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await WhenTcsCompletes(tcs);
        await tcs.Task;

        // UnsettledMessageCount should always be 0 for pre-settled consumers
        Assert.Equal(0, consumer.UnsettledMessageCount);

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task NonPreSettledConsumerUnsettledMessageCountIncrements()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        const int messageCount = 3;
        await PublishAsync(queueSpec, messageCount);

        TaskCompletionSource<bool> tcs = new();
        int receivedCount = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.DefaultSettle) // Explicitly avoid pre-settled feature
            .MessageHandler((context, message) =>
            {
                receivedCount++;
                // Don't accept, so messages remain unsettled
                if (receivedCount == messageCount)
                {
                    tcs.SetResult(true);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await WhenTcsCompletes(tcs);
        await tcs.Task;

        // UnsettledMessageCount should equal the number of messages received but not settled
        Assert.Equal(messageCount, consumer.UnsettledMessageCount);

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task PreSettledConsumerCanReceiveMultipleMessages()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        const int messageCount = 10;
        await PublishAsync(queueSpec, messageCount);

        TaskCompletionSource<bool> tcs = new();
        int receivedCount = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .Feature(ConsumerFeature.PreSettled)
            .MessageHandler((context, message) =>
            {
                receivedCount++;
                // Messages are pre-settled, no need to call Accept()
                if (receivedCount == messageCount)
                {
                    tcs.SetResult(true);
                }

                return Task.CompletedTask;
            })
            .BuildAndStartAsync();

        await WhenTcsCompletes(tcs);
        await tcs.Task;

        Assert.Equal(messageCount, receivedCount);
        Assert.Equal(0, consumer.UnsettledMessageCount);
        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal((ulong)0, queueInfo.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }
}
