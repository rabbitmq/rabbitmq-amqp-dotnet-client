// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

#if NET8_0_OR_GREATER
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

public class ConsumerMetricsTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task RecordMessageDeliverySuccessAndDuration()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        Assert.NotNull(_meterFactory);
        Assert.NotNull(_metricsReporter);

        var messageConsumedCollector =
            new MetricCollector<int>(_meterFactory, "RabbitMQ.Amqp", "messaging.client.consumed.messages");
        var messageProcessingDurationCollector =
            new MetricCollector<double>(_meterFactory, "RabbitMQ.Amqp", "messaging.process.duration");

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 2);

        TaskCompletionSource<IMessage> tcs = new();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .MessageHandler(async (context, message) =>
                {
                    await context.AcceptAsync();
                    tcs.SetResult(message);
                }
            ).BuildAndStartAsync();

        await WhenTcsCompletes(tcs);

        var consumedMessagesMeasurements = messageConsumedCollector.GetMeasurementSnapshot();
        Assert.NotEmpty(consumedMessagesMeasurements);
        Assert.Equal(1, consumedMessagesMeasurements[0].Value);
        Assert.Equal(consumedMessagesMeasurements[0].Tags["messaging.system"], "rabbitmq");
        Assert.Equal(consumedMessagesMeasurements[0].Tags["messaging.operation.name"], "deliver");
        Assert.Equal(consumedMessagesMeasurements[0].Tags["messaging.operation.type"], "process");
        Assert.Equal(consumedMessagesMeasurements[0].Tags["messaging.destination.name"],
            $"/queues/{Utils.EncodePathSegment(queueSpec.QueueName)}");
        Assert.Equal(consumedMessagesMeasurements[0].Tags["server.port"],
            _connectionSettings!.Port);
        Assert.Equal(consumedMessagesMeasurements[0].Tags["server.address"],
            _connectionSettings!.Host);

        var consumedMessageDurationMeasurements = messageProcessingDurationCollector.GetMeasurementSnapshot();
        Assert.NotEmpty(consumedMessageDurationMeasurements);
        Assert.True(consumedMessageDurationMeasurements[0].Value > 0);
        Assert.Equal(consumedMessageDurationMeasurements[0].Tags["messaging.system"], "rabbitmq");
        Assert.Equal(consumedMessageDurationMeasurements[0].Tags["messaging.operation.name"], "deliver");
        Assert.Equal(consumedMessageDurationMeasurements[0].Tags["messaging.operation.type"], "process");
        Assert.Equal(consumedMessageDurationMeasurements[0].Tags["messaging.destination.name"],
            $"/queues/{Utils.EncodePathSegment(queueSpec.QueueName)}");
        Assert.Equal(consumedMessageDurationMeasurements[0].Tags["server.port"],
            _connectionSettings!.Port);
        Assert.Equal(consumedMessageDurationMeasurements[0].Tags["server.address"],
            _connectionSettings!.Host);

        await consumer.CloseAsync();
        consumer.Dispose();
    }
}

#else
#endif
