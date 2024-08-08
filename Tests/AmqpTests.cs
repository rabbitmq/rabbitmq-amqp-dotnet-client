// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class AmqpTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task QueueInfoTest()
    {
        string queueName = _testDisplayName;
        Assert.NotNull(_connection);

        IManagement management = _connection.Management();

        IQueueInfo declaredQueueInfo = await management.Queue(queueName).Quorum().Queue().Declare();
        IQueueInfo retrievedQueueInfo = await management.GetQueueInfoAsync(queueName);

        Assert.Equal(queueName, declaredQueueInfo.Name());
        Assert.Equal(queueName, retrievedQueueInfo.Name());

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
}
