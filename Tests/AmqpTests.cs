// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class AmqpTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    private readonly byte[] _messageBody = Encoding.UTF8.GetBytes("hello");

    [Theory]
    [InlineData(QueueType.CLASSIC, "classic")]
    [InlineData(QueueType.QUORUM, "quorum")]
    public async Task QueueInfoTest(QueueType expectedQueueType, string expectedQueueTypeName)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpecification = _management.Queue(_queueName).Type(expectedQueueType);

        IQueueInfo declaredQueueInfo = await queueSpecification.DeclareAsync();
        IQueueInfo retrievedQueueInfo = await _management.GetQueueInfoAsync(_queueName);

        Assert.Equal(_queueName, declaredQueueInfo.Name());
        Assert.Equal(_queueName, retrievedQueueInfo.Name());

        Assert.Equal(expectedQueueType, declaredQueueInfo.Type());
        Assert.Equal(expectedQueueType, retrievedQueueInfo.Type());

        Assert.True(declaredQueueInfo.Durable());
        Assert.True(retrievedQueueInfo.Durable());

        Assert.False(declaredQueueInfo.AutoDelete());
        Assert.False(retrievedQueueInfo.AutoDelete());

        Assert.False(declaredQueueInfo.Exclusive());
        Assert.False(retrievedQueueInfo.Exclusive());

        Assert.Equal((ulong)0, declaredQueueInfo.MessageCount());
        Assert.Equal((ulong)0, retrievedQueueInfo.MessageCount());

        Assert.Equal((ulong)0, declaredQueueInfo.ConsumerCount());
        Assert.Equal((ulong)0, retrievedQueueInfo.ConsumerCount());

        Dictionary<string, object> declaredArgs = declaredQueueInfo.Arguments();
        Dictionary<string, object> retrievedArgs = retrievedQueueInfo.Arguments();

        Assert.True(declaredArgs.ContainsKey("x-queue-type"));
        Assert.True(retrievedArgs.ContainsKey("x-queue-type"));
        Assert.Equal(declaredArgs["x-queue-type"], expectedQueueTypeName);
        Assert.Equal(retrievedArgs["x-queue-type"], expectedQueueTypeName);
    }

    [Theory]
    [InlineData("foobar", QueueType.CLASSIC)]
    [InlineData("foobar", QueueType.QUORUM)]
    [InlineData("фообар", QueueType.CLASSIC)]
    [InlineData("фообар", QueueType.QUORUM)]
    public async Task QueueDeclareDeletePublishConsume(string subject, QueueType expectedQueueType)
    {
        byte[] messageBody = Encoding.UTF8.GetBytes("hello");
        const int messageCount = 100;

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification? queueSpecification = null;
        switch (expectedQueueType)
        {
            case QueueType.CLASSIC:
                queueSpecification = _management.Queue().Name(_queueName).Classic().Queue();
                break;
            case QueueType.QUORUM:
                queueSpecification = _management.Queue().Name(_queueName).Quorum().Queue();
                break;
            default:
                Assert.Fail();
                break;
        }

        IQueueInfo declaredQueueInfo = await queueSpecification.DeclareAsync();
        Assert.Equal(_queueName, declaredQueueInfo.Name());

        void ml(ulong idx, IMessage msg)
        {
            msg.MessageId(idx);
            msg.Subject(subject);
        }
        await PublishAsync(queueSpecification, messageCount, ml);

        IQueueInfo retrievedQueueInfo0 = await _management.GetQueueInfoAsync(_queueName);
        Assert.Equal(_queueName, retrievedQueueInfo0.Name());
        Assert.Equal((uint)0, retrievedQueueInfo0.ConsumerCount());
        Assert.Equal((ulong)messageCount, retrievedQueueInfo0.MessageCount());

        long receivedMessageCount = 0;
        TaskCompletionSource<bool> allMessagesReceivedTcs = CreateTaskCompletionSource();
        string? receivedSubject = null;
        var messageIds = new ConcurrentBag<ulong>();
        Task MessageHandler(IContext ctx, IMessage msg)
        {
            try
            {
                receivedSubject = msg.Subject();
                messageIds.Add((ulong)msg.MessageId());
                ctx.Accept();
                if (Interlocked.Increment(ref receivedMessageCount) == messageCount)
                {
                    allMessagesReceivedTcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                allMessagesReceivedTcs.SetException(ex);
            }
            return Task.CompletedTask;
        }

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder();
        IConsumer consumer = await consumerBuilder.Queue(_queueName).MessageHandler(MessageHandler).BuildAndStartAsync();

        await WhenTaskCompletes(allMessagesReceivedTcs.Task);
        Assert.Equal(messageCount, messageIds.Count);

        Assert.NotNull(receivedSubject);
        Assert.Equal(subject, receivedSubject);

        IQueueInfo retrievedQueueInfo1 = await _management.GetQueueInfoAsync(_queueName);
        Assert.Equal((uint)1, retrievedQueueInfo1.ConsumerCount());
        Assert.Equal((uint)0, retrievedQueueInfo1.MessageCount());

        await consumer.CloseAsync();
        consumer.Dispose();
    }

    [Theory]
    [InlineData("foo", false)]
    [InlineData("foo", true)]
    [InlineData("фообар", true)]
    [InlineData("фообар", false)]
    [InlineData("фоо!бар", false)]
    [InlineData("фоо!бар", true)]
    public async Task BindingTest(string prefix, bool addBindingArgments)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        string now = Now;
        string rkStr = $"{prefix}-foo-{now}";

        Dictionary<string, object> bindingArguments = new();
        if (addBindingArgments)
        {
            bindingArguments.Add("foo", prefix + "-bar");
        }

        IExchangeSpecification ex1spec = _management.Exchange($"{prefix}-e1-{_testDisplayName}-{now}").Type(ExchangeType.DIRECT);
        IExchangeSpecification ex2spec = _management.Exchange($"{prefix}-e2-{_testDisplayName}-{now}").Type(ExchangeType.FANOUT);
        IQueueSpecification queueSpec = _management.Queue(_queueName).Type(QueueType.CLASSIC);

        await ex1spec.DeclareAsync();
        await ex2spec.DeclareAsync();
        await queueSpec.DeclareAsync();

        IBindingSpecification e1e2Binding = _management.Binding()
            .SourceExchange(ex1spec)
            .DestinationExchange(ex2spec)
            .Key(rkStr)
            .Arguments(bindingArguments);
        IBindingSpecification e2qBinding = _management.Binding()
            .SourceExchange(ex2spec)
            .DestinationQueue(queueSpec)
            .Arguments(bindingArguments);

        await e1e2Binding.BindAsync();
        await e2qBinding.BindAsync();

        IPublisherBuilder publisherBuilder1 = _connection.PublisherBuilder();
        IPublisherBuilder publisherBuilder2 = _connection.PublisherBuilder();

        IPublisher publisher1 = await publisherBuilder1.Exchange(ex1spec).Key(rkStr).BuildAsync();
        IPublisher publisher2 = await publisherBuilder2.Exchange(ex2spec).BuildAsync();

        IMessage message = new AmqpMessage(_messageBody);

        Task<PublishResult> publish1Task = publisher1.PublishAsync(message);
        Task<PublishResult> publish2Task = publisher2.PublishAsync(message);
        await WhenAllComplete([publish1Task, publish2Task]);

        PublishResult publish1Result = await publish1Task;
        Assert.Equal(OutcomeState.Accepted, publish1Result.Outcome.State);

        PublishResult publish2Result = await publish2Task;
        Assert.Equal(OutcomeState.Accepted, publish2Result.Outcome.State);

        const int expectedMessageCount = 2;
        long receivedMessageCount = 0;
        TaskCompletionSource<bool> allMessagesReceivedTcs = CreateTaskCompletionSource();
        Task MessageHandler(IContext ctx, IMessage msg)
        {
            try
            {
                ctx.Accept();
                if (Interlocked.Increment(ref receivedMessageCount) == expectedMessageCount)
                {
                    allMessagesReceivedTcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                allMessagesReceivedTcs.SetException(ex);
            }
            return Task.CompletedTask;
        }

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder();
        IConsumer consumer = await consumerBuilder.Queue(_queueName).MessageHandler(MessageHandler).BuildAndStartAsync();

        await WhenTaskCompletes(allMessagesReceivedTcs.Task);

        await publisher1.CloseAsync();
        publisher1.Dispose();

        await publisher2.CloseAsync();
        publisher2.Dispose();

        await consumer.CloseAsync();
        consumer.Dispose();

        await e1e2Binding.UnbindAsync();
        await e2qBinding.UnbindAsync();

        await ex2spec.DeleteAsync();
        await ex2spec.DeleteAsync();
        // Note: DisposeAsync will delete the queue
    }

    [Fact]
    public async Task SameTypeMessagesInQueue()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpecification = _management.Queue(_queueName).Exclusive(true);
        IQueueInfo declaredQueueInfo = await queueSpecification.DeclareAsync();

        var messageBodies = new ConcurrentBag<string>();
        const int expectedMessageCount = 2;
        long receivedMessageCount = 0;
        TaskCompletionSource<bool> allMessagesReceivedTcs = CreateTaskCompletionSource();
        Task MessageHandler(IContext ctx, IMessage msg)
        {
            try
            {
                ctx.Accept();
                messageBodies.Add(Encoding.UTF8.GetString((byte[])msg.Body()));
                if (Interlocked.Increment(ref receivedMessageCount) == expectedMessageCount)
                {
                    allMessagesReceivedTcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                allMessagesReceivedTcs.SetException(ex);
            }
            return Task.CompletedTask;
        }

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder();
        IConsumer consumer = await consumerBuilder.Queue(queueSpecification).MessageHandler(MessageHandler).BuildAndStartAsync();

        IPublisherBuilder publisherBuilder = _connection.PublisherBuilder();
        IPublisher publisher = await publisherBuilder.Queue(queueSpecification).BuildAsync();

        IMessage message1 = new AmqpMessage(_messageBody);
        IMessage message2 = new AmqpMessage(Encoding.UTF8.GetBytes("world"));

        Task<PublishResult> publish1Task = publisher.PublishAsync(message1);
        Task<PublishResult> publish2Task = publisher.PublishAsync(message2);
        await WhenAllComplete([publish1Task, publish2Task]);

        PublishResult publish1Result = await publish1Task;
        Assert.Equal(OutcomeState.Accepted, publish1Result.Outcome.State);
        PublishResult publish2Result = await publish2Task;
        Assert.Equal(OutcomeState.Accepted, publish2Result.Outcome.State);

        await WhenTaskCompletes(allMessagesReceivedTcs.Task);

        Assert.Contains("hello", messageBodies);
        Assert.Contains("world", messageBodies);
    }
}
