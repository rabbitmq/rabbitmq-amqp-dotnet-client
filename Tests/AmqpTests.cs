// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class AmqpTests : IntegrationTest
{
    private readonly string _queueName;

    public AmqpTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
        _queueName = $"{_testDisplayName}-queue-{Now}";
    }

    [Fact]
    public async Task QueueInfoTest()
    {
        Assert.NotNull(_connection);

        IManagement management = _connection.Management();

        IQueueInfo declaredQueueInfo = await management.Queue(_queueName).Quorum().Queue().Declare();
        IQueueInfo retrievedQueueInfo = await management.GetQueueInfoAsync(_queueName);

        Assert.Equal(_queueName, declaredQueueInfo.Name());
        Assert.Equal(_queueName, retrievedQueueInfo.Name());

        Assert.Equal(QueueType.QUORUM, declaredQueueInfo.Type());
        Assert.Equal(QueueType.QUORUM, retrievedQueueInfo.Type());

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
        Assert.Equal(declaredArgs["x-queue-type"], "quorum");
        Assert.Equal(retrievedArgs["x-queue-type"], "quorum");
    }

    [Theory]
    [InlineData("foobar")]
    [InlineData("фообар")]
    public async Task QueueDeclareDeletePublishConsume(string subject)
    {
        byte[] messageBody = Encoding.UTF8.GetBytes("hello");
        const int messageCount = 100;

        Assert.NotNull(_connection);

        IManagement management = _connection.Management();

        // IQueueInfo declaredQueueInfo = await management.Queue().Name(_queueName).Quorum().Queue().Declare();
        IQueueInfo declaredQueueInfo = await management.Queue().Name(_queueName).Classic().Queue().Declare();
        Assert.Equal(_queueName, declaredQueueInfo.Name());

        IPublisherBuilder publisherBuilder = _connection.PublisherBuilder();
        IPublisher publisher = await publisherBuilder.Queue(declaredQueueInfo.Name()).BuildAsync();

        long publishedMessageCount = 0;
        var publishTasks = new List<Task>();
        var allMessagesPublishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        for (int i = 0; i < messageCount; i++)
        {
            Guid messageId = Guid.NewGuid();

            IMessage message = new AmqpMessage(messageBody);
            message.MessageId(messageId.ToString());
            message.Subject(subject);

            publishTasks.Add(publisher.Publish(message, (IMessage msg, OutcomeDescriptor outcome) =>
            {
                if (outcome.State == OutcomeState.Accepted)
                {
                    if (Interlocked.Increment(ref publishedMessageCount) == messageCount)
                    {
                        allMessagesPublishedTcs.SetResult();
                    }
                }
            }));
        }

        await WhenAllComplete(publishTasks);
        await WhenTaskCompletes(allMessagesPublishedTcs.Task);

        Assert.Equal(messageCount, publishedMessageCount);

        IQueueInfo retrievedQueueInfo0 = await management.GetQueueInfoAsync(_queueName);
        Assert.Equal(_queueName, retrievedQueueInfo0.Name());
        Assert.Equal((uint)0, retrievedQueueInfo0.ConsumerCount());
        Assert.Equal((ulong)messageCount, retrievedQueueInfo0.MessageCount());

        long receivedMessageCount = 0;
        var allMessagesReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        string? receivedSubject = null;
        void MessageHandler(IContext ctx, IMessage msg)
        {
            receivedSubject = msg.Subject();
            ctx.Accept();
            if (Interlocked.Increment(ref receivedMessageCount) == messageCount)
            {
                allMessagesReceivedTcs.SetResult();
            }
        }

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder();
        IConsumer consumer = await consumerBuilder.Queue(_queueName).MessageHandler(MessageHandler).BuildAsync();

        await WhenTaskCompletes(allMessagesReceivedTcs.Task);

        Assert.NotNull(receivedSubject);
        Assert.Equal(subject, receivedSubject);

        IQueueInfo retrievedQueueInfo1 = await management.GetQueueInfoAsync(_queueName);
        Assert.Equal((uint)1, retrievedQueueInfo1.ConsumerCount());
        Assert.Equal((uint)0, retrievedQueueInfo1.MessageCount());
    }
}
