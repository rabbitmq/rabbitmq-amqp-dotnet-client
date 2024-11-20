// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

public class BasicConsumerTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task SimpleConsumeMessage()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 1);

        TaskCompletionSource<IMessage> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .MessageHandler((context, message) =>
            {
                context.Accept();
                tcs.SetResult(message);
                return Task.CompletedTask;
            }
        ).BuildAndStartAsync();

        await WhenTcsCompletes(tcs);
        IMessage receivedMessage = await tcs.Task;
        Assert.Equal("message_0", receivedMessage.Body());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    /// <summary>
    /// Test the Requeue method of the IContext interface
    /// The first time the message is requeued, the second time it is accepted
    /// It is a bit tricky to test the requeue method, because the message is requeued asynchronously
    /// but this simple use case should be enough to test the requeue method
    /// </summary>
    [Fact]
    public async Task ConsumerReQueueMessage()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpecification = _management.Queue(_queueName);
        await queueSpecification.DeclareAsync();

        await PublishAsync(queueSpecification, 1);

        TaskCompletionSource<int> tcs = new();
        int consumed = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpecification)
            .MessageHandler((context, message) =>
            {
                Assert.Equal("message_0", message.Body());
                Interlocked.Increment(ref consumed);
                switch (consumed)
                {
                    case 1:
                        // first time requeue the message
                        // it must consume again
                        context.Requeue();
                        break;
                    case 2:
                        context.Accept();
                        tcs.SetResult(consumed);
                        break;
                }
                return Task.CompletedTask;
            }
        ).BuildAndStartAsync();

        await WhenTcsCompletes(tcs);

        await consumer.CloseAsync();
        consumer.Dispose();

        await SystemUtils.WaitUntilQueueMessageCount(queueSpecification, 0);
        await queueSpecification.DeleteAsync();
    }

    [Fact]
    public async Task ConsumerRejectOnlySomeMessage()
    {
        const int publishCount = 500;
        const int initialCredits = 100;

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue(_queueName);
        await queueSpec.DeclareAsync();

        IConsumer? consumer = null;
        try
        {
            await PublishAsync(queueSpec, publishCount);

            TaskCompletionSource<List<IMessage>> tcs = new();
            int messagesConsumedCount = 0;
            List<IMessage> receivedMessages = new();
            Task MessageHandler(IContext cxt, IMessage msg)
            {
                receivedMessages.Add(msg);

                Interlocked.Increment(ref messagesConsumedCount);

                if (messagesConsumedCount % 2 == 0)
                {
                    cxt.Discard();
                }
                else
                {
                    cxt.Accept();
                }

                if (messagesConsumedCount == publishCount)
                {
                    tcs.SetResult(receivedMessages);
                }

                return Task.CompletedTask;
            }

            consumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .InitialCredits(initialCredits)
                .MessageHandler(MessageHandler).BuildAndStartAsync();

            await WhenTcsCompletes(tcs);

            List<IMessage> receivedMessagesFromTask = await tcs.Task;

            Assert.Equal(publishCount, receivedMessagesFromTask.Count);

            for (int i = 0; i < publishCount; i++)
            {
                if (i % 2 == 0)
                {
                    Assert.Equal($"message_{i}", receivedMessagesFromTask[i].Body());
                }
            }
        }
        finally
        {
            if (consumer is not null)
            {
                await consumer.CloseAsync();
                consumer.Dispose();
            }
        }
    }

    /// <summary>
    /// Test the consumer for a stream queue with offset
    /// The test is not deterministic because we don't know how many messages will be consumed
    /// We assume that the messages consumed are greater than or equal to the expected number of messages
    /// </summary>
    /// <param name="offset"></param>
    /// <param name="numberOfMessagesExpected"></param>
    [Theory]
    [InlineData(StreamOffsetSpecification.First, 100)]
    [InlineData(StreamOffsetSpecification.Last, 1)]
    [InlineData(StreamOffsetSpecification.Next, 0)]
    public async Task ConsumerForStreamQueueWithOffset(StreamOffsetSpecification offset, int numberOfMessagesExpected)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.STREAM);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 100);

        int consumed = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .InitialCredits(100)
            .MessageHandler((context, message) =>
            {
                Interlocked.Increment(ref consumed);
                return Task.CompletedTask;
            })
            .Stream()
            .Offset(offset)
            .Builder()
            .BuildAndStartAsync();

        // wait for the consumer to consume all messages
        // we can't use the TaskCompletionSource here because we don't know how many messages will be consumed
        // In two seconds, the consumer should consume all messages
        await SystemUtils.WaitUntilFuncAsync(() => consumed >= numberOfMessagesExpected);

        // we don't know how many messages will be consumed
        // expect for the case FIRST
        // we just assume that the messages consumed are greater than or equal to the expected number of messages
        // For example in case of "LAST" we expect 1 message to be consumed, but it could be more
        Assert.True(consumed >= numberOfMessagesExpected);

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    /// <summary>
    /// Test for stream filtering
    /// There are two consumers:
    /// - one with a filter that should receive only the messages with the filter
    /// - one without filter that should receive all messages
    /// </summary>
    /// <param name="argStreamFilterValues"></param>
    /// <param name="expected"></param>
    [Theory]
    [InlineData("pizza,beer,pasta,wine", 4)]
    [InlineData("pizza,beer", 2)]
    [InlineData("pizza", 1)]
    public async Task ConsumerWithStreamFilterShouldReceiveOnlyPartOfTheMessages(
        string argStreamFilterValues, int expected)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        string[] streamFilterValues = argStreamFilterValues.Split(',');

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.STREAM);
        await queueSpec.DeclareAsync();

        var publishTasks = new List<Task>();
        foreach (string sfv in streamFilterValues)
        {
            void ml(ulong idx, IMessage msg)
            {
                msg.MessageId(idx);
                msg.Annotation("x-stream-filter-value", sfv);
            }
            publishTasks.Add(PublishAsync(queueSpec, 1, ml));

        }
        await WhenAllComplete(publishTasks);
        publishTasks.Clear();

        // wait for the messages to be published and the chunks to be created
        await Task.Delay(TimeSpan.FromSeconds(1)); // TODO better way to do this?

        // publish extra messages without filter and these messages should be always excluded
        // by the consumer with the filter
        await PublishAsync(queueSpec, 10);

        List<IMessage> receivedMessages = [];
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .InitialCredits(100)
            .MessageHandler((context, message) =>
            {
                receivedMessages.Add(message);
                context.Accept();
                return Task.CompletedTask;
            })
            .Stream()
            .FilterValues(streamFilterValues)
            .FilterMatchUnfiltered(false)
            .Offset(StreamOffsetSpecification.First).Builder()
            .BuildAndStartAsync();

        int receivedWithoutFilters = 0;
        IConsumer consumerWithoutFilters = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .InitialCredits(100)
            .MessageHandler((context, message) =>
            {
                Interlocked.Increment(ref receivedWithoutFilters);
                context.Accept();
                return Task.CompletedTask;
            })
            .Stream()
            .Offset(StreamOffsetSpecification.First).Builder()
            .BuildAndStartAsync();

        // wait for the consumer to consume all messages
        await Task.Delay(500); // TODO yuck

        Assert.Equal(expected, receivedMessages.Count);
        Assert.Equal(streamFilterValues.Length + 10, receivedWithoutFilters);

        await consumer.CloseAsync();
        consumer.Dispose();

        await consumerWithoutFilters.CloseAsync();
        consumerWithoutFilters.Dispose();
    }

    /// <summary>
    /// Test the offset value for the stream queue
    /// </summary>
    /// <param name="offsetStart"></param>
    /// <param name="numberOfMessagesExpected"></param>
    [Theory]
    [InlineData(0, 100)]
    [InlineData(50, 50)]
    [InlineData(99, 1)]
    public async Task ConsumerForStreamQueueWithOffsetValue(int offsetStart, int numberOfMessagesExpected)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.STREAM);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 100);

        int consumed = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .InitialCredits(100)
            .MessageHandler((context, message) =>
            {
                Interlocked.Increment(ref consumed);
                return Task.CompletedTask;
            })
            .Stream()
            .Offset(offsetStart)
            .Builder()
            .BuildAndStartAsync();

        // wait for the consumer to consume all messages
        // we can't use the TaskCompletionSource here because we don't know how many messages will be consumed
        // In two seconds, the consumer should consume all messages
        await Task.Delay(TimeSpan.FromSeconds(2)); // TODO

        Assert.Equal(consumed, numberOfMessagesExpected);

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task ConsumerShouldThrowWhenQueueDoesNotExist()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        string doesNotExist = Guid.NewGuid().ToString();

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder()
            .Queue(doesNotExist)
            .MessageHandler((context, message) =>
            {
                return Task.CompletedTask;
            }
        );

        // TODO these are timeout exceptions under the hood, compare
        // with the Java client
        ConsumerException ex = await Assert.ThrowsAsync<ConsumerException>(
            () => consumerBuilder.BuildAndStartAsync());
        Assert.Contains(doesNotExist, ex.Message);
    }

    [Fact]
    public async Task ConsumerShouldBeClosedWhenQueueIsDeleted()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpecification = _management.Queue(_queueName).Exclusive(true);
        IQueueInfo queueInfo = await queueSpecification.DeclareAsync();
        Assert.Equal(_queueName, queueInfo.Name());

        TaskCompletionSource<bool> messageHandledTcs = CreateTaskCompletionSource();
        Task MessageHandler(IContext cxt, IMessage msg)
        {
            cxt.Accept();
            messageHandledTcs.SetResult(true);
            return Task.CompletedTask;
        }

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder()
            .Queue(_queueName)
            .MessageHandler(MessageHandler);
        IConsumer consumer = await consumerBuilder.BuildAndStartAsync();

        await PublishAsync(queueSpecification, 1);

        await WhenTcsCompletes(messageHandledTcs);

        await queueSpecification.DeleteAsync();

        // TODO here is where the consumer Listener should see a closed event

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Fact]
    public async Task ConsumerUnsettledMessagesGoBackToQueueAfterClosing()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 100;
        int initialCredits = messageCount / 10;
        int settledCount = initialCredits * 2;

        IQueueSpecification queueSpecification = _management.Queue(_queueName).Exclusive(true);
        IQueueInfo queueInfo0 = await queueSpecification.DeclareAsync();
        Assert.Equal(_queueName, queueInfo0.Name());

        await PublishAsync(queueSpecification, messageCount);

        TaskCompletionSource<bool> receivedGreaterThanSettledTcs = CreateTaskCompletionSource();
        long receivedCount = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueInfo0.Name())
            .InitialCredits(initialCredits)
            .MessageHandler((IContext ctx, IMessage msg) =>
            {
                if (Interlocked.Increment(ref receivedCount) <= settledCount)
                {
                    ctx.Accept();
                }
                else
                {
                    receivedGreaterThanSettledTcs.TrySetResult(true);
                }
                return Task.CompletedTask;
            }).BuildAndStartAsync();

        await WhenTcsCompletes(receivedGreaterThanSettledTcs);

        await consumer.CloseAsync();

        IQueueInfo queueInfo1 = await _management.GetQueueInfoAsync(queueSpecification);
        ulong expectedMessageCount = (ulong)(messageCount - settledCount);
        Assert.Equal(expectedMessageCount, queueInfo1.MessageCount());
    }

    [Fact]
    public async Task ConsumerWithHigherPriorityShouldGetMessagesFirst()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 100;

        int lowPriorityReceivedCount = 0;
        int highPriorityReceivedCount = 0;
        int receivedCount = 0;

        IQueueSpecification queueSpecification = _management.Queue(_queueName).Exclusive(true);
        IQueueInfo queueInfo = await queueSpecification.DeclareAsync();

        TaskCompletionSource<bool> allMessagesReceivedTcs = CreateTaskCompletionSource();
        IConsumerBuilder lowPriorityConsumerBuilder = _connection.ConsumerBuilder()
            .Queue(queueSpecification)
            // .Priority(1) TODO
            .MessageHandler((IContext cxt, IMessage msg) =>
            {
                try
                {
                    cxt.Accept();
                    Interlocked.Increment(ref lowPriorityReceivedCount);
                    if (Interlocked.Increment(ref receivedCount) == messageCount)
                    {
                        allMessagesReceivedTcs.SetResult(true);
                    }
                }
                catch (Exception ex)
                {
                    allMessagesReceivedTcs.SetException(ex);
                }
                return Task.CompletedTask;
            });
        IConsumer lowPriorityConsumer = await lowPriorityConsumerBuilder.BuildAndStartAsync();

        IConsumerBuilder highPriorityConsumerBuilder = _connection.ConsumerBuilder()
            .Queue(queueSpecification)
            // .Priority(5) TODO
            .MessageHandler((IContext cxt, IMessage msg) =>
            {
                try
                {
                    cxt.Accept();
                    Interlocked.Increment(ref highPriorityReceivedCount);
                    if (Interlocked.Increment(ref receivedCount) == messageCount)
                    {
                        allMessagesReceivedTcs.SetResult(true);
                    }
                }
                catch (Exception ex)
                {
                    allMessagesReceivedTcs.SetException(ex);
                }
                return Task.CompletedTask;
            });
        IConsumer highPriorityConsumer = await highPriorityConsumerBuilder.BuildAndStartAsync();

        await PublishAsync(queueSpecification, messageCount);

        await WhenTcsCompletes(allMessagesReceivedTcs);

        await lowPriorityConsumer.CloseAsync();
        await highPriorityConsumer.CloseAsync();
        lowPriorityConsumer.Dispose();
        highPriorityConsumer.Dispose();
    }
    /*
     * TODO
    assertThat(lowCount).hasValue(0);
    assertThat(highCount).hasValue(messageCount);
    highPriorityConsumer.close();
    consumeSync.reset(messageCount);
    publish.run();
    Assertions.assertThat(consumeSync).completes();
    assertThat(lowCount).hasValue(messageCount);
    assertThat(highCount).hasValue(messageCount);
    lowPriorityConsumer.close();
    */
}
