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

namespace Tests.Consumer
{
    public class ConsumerDispositionTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        [Fact]
        public async Task BatchAcceptDisposition()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
            await queueSpec.DeclareAsync();
            const int batchSize = 18;
            await PublishAsync(queueSpec, batchSize);
            BatchDeliveryContext batch = new();
            TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, _) =>
                {
                    Assert.NotNull(batch);
                    batch.Add(context);
                    if (batch.Count() != batchSize)
                    {
                        return Task.CompletedTask;
                    }

                    batch.Accept();
                    tcs.SetResult(true);

                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            Assert.NotNull(consumer);
            await tcs.Task;

            Assert.Equal(0, consumer.UnsettledMessageCount);
            await WaitUntilQueueMessageCount(queueSpec, 0);
            await queueSpec.DeleteAsync();
            await consumer.CloseAsync();
        }

        [Fact]
        public async Task BatchDiscardDisposition()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
            await queueSpec.DeclareAsync();
            const int batchSize = 18;
            await PublishAsync(queueSpec, batchSize);
            BatchDeliveryContext batch = new();
            TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, _) =>
                {
                    Assert.NotNull(batch);
                    batch.Add(context);
                    if (batch.Count() != batchSize)
                    {
                        return Task.CompletedTask;
                    }

                    batch.Discard();
                    tcs.SetResult(true);

                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            Assert.NotNull(consumer);
            await tcs.Task;

            Assert.Equal(0, consumer.UnsettledMessageCount);
            await WaitUntilQueueMessageCount(queueSpec, 0);
            await queueSpec.DeleteAsync();
            await consumer.CloseAsync();
        }

        [Fact]
        public async Task BatchDiscardAnnotationDisposition()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
            await queueSpec.DeclareAsync();
            const int batchSize = 18;
            await PublishAsync(queueSpec, batchSize);
            BatchDeliveryContext batch = new();
            TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, _) =>
                {
                    Assert.NotNull(batch);
                    batch.Add(context);
                    if (batch.Count() != batchSize)
                    {
                        return Task.CompletedTask;
                    }

                    batch.Discard(new Dictionary<string, object>()
                    {
                        { "x-opt-annotation-key", "annotation-value" },
                        { "x-opt-annotation1-key", "annotation1-value" }
                    });
                    tcs.SetResult(true);

                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            Assert.NotNull(consumer);
            await tcs.Task;

            Assert.Equal(0, consumer.UnsettledMessageCount);
            await WaitUntilQueueMessageCount(queueSpec, 0);
            await queueSpec.DeleteAsync();
            await consumer.CloseAsync();
        }

        [Fact]
        public async Task BatchRequeueDisposition()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
            await queueSpec.DeclareAsync();
            const int batchSize = 18;
            await PublishAsync(queueSpec, batchSize);
            BatchDeliveryContext batch = new();
            TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, _) =>
                {
                    Assert.NotNull(batch);
                    batch.Add(context);
                    if (batch.Count() != batchSize)
                    {
                        return Task.CompletedTask;
                    }

                    batch.Requeue();
                    tcs.SetResult(true);

                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            Assert.NotNull(consumer);
            await tcs.Task;
            await consumer.CloseAsync();
            Assert.Equal(0, consumer.UnsettledMessageCount);
            await WaitUntilQueueMessageCount(queueSpec, 18);
            await queueSpec.DeleteAsync();
        }

        [Fact]
        public async Task BatchRequeueAnnotationsDisposition()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
            await queueSpec.DeclareAsync();
            const int batchSize = 18;
            await PublishAsync(queueSpec, batchSize);
            BatchDeliveryContext batch = new();
            TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, _) =>
                {
                    Assert.NotNull(batch);
                    batch.Add(context);
                    if (batch.Count() != batchSize)
                    {
                        return Task.CompletedTask;
                    }

                    const string annotationKey = "x-opt-annotation-key";
                    const string annotationValue = "annotation-value";

                    const string annotationKey1 = "x-opt-annotation1-key";
                    const string annotationValue1 = "annotation1-value";
                    Assert.Equal(batchSize, batch.Count());
                    batch.Requeue(new Dictionary<string, object>()
                    {
                        { annotationKey, annotationValue }, { annotationKey1, annotationValue1 }
                    });
                    Assert.Equal(0, batch.Count());
                    
                    tcs.SetResult(true);

                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            Assert.NotNull(consumer);
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(20));
            await consumer.CloseAsync();
            await WaitUntilQueueMessageCount(queueSpec, 18);
            await queueSpec.DeleteAsync();
        }

        [Fact]
        public async Task MixBatchAcceptAndDiscardDisposition()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
            await queueSpec.DeclareAsync();
            const int batchSize = 18;
            await PublishAsync(queueSpec, batchSize * 2);
            BatchDeliveryContext batch = new();
            TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            bool acceptNext = true; // Flag to alternate between accept and discard
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, _) =>
                {
                    Assert.NotNull(batch);
                    batch.Add(context);
                    if (batch.Count() == batchSize && acceptNext)
                    {
                        Assert.Equal(batchSize, batch.Count());
                        batch.Accept();
                        acceptNext = false; // Switch to discard next
                    }
                    else if (batch.Count() == batchSize && !acceptNext)
                    {
                        Assert.Equal(batchSize, batch.Count());
                        batch.Discard();
                        tcs.SetResult(true);
                    }
                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            Assert.NotNull(consumer);
            await tcs.Task;

            Assert.Equal(0, consumer.UnsettledMessageCount);
            await WaitUntilQueueMessageCount(queueSpec, 0);
            await queueSpec.DeleteAsync();
            await consumer.CloseAsync();
        }
    }
}
