// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

public class ConsumerMetricsTests : IntegrationTest, IMeterFactory
{
    private const string MetricPrefix = "rabbitmq.amqp";
    private readonly MetricsReporter _metricsReporter;
    private Meter? _meter;

    public ConsumerMetricsTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, setupConnectionAndManagement: false)
    {
        _metricsReporter = new MetricsReporter(this);
    }

    public Meter Create(MeterOptions options)
    {
        _meter = new Meter(options);
        return _meter;
    }

    public void Dispose() => _meter?.Dispose();

    public override async Task InitializeAsync()
    {
        Assert.Null(_connection);
        Assert.Null(_management);

        _connectionSettings = _connectionSettingBuilder.Build();
        IEnvironment environment = AmqpEnvironment.Create(_connectionSettings, _metricsReporter);
        _connection = await environment.CreateConnectionAsync();
        _management = _connection.Management();
    }

    [Fact]
    public async Task RecordMessageDeliverySuccessAndDuration()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        Assert.NotNull(_metricsReporter);

        var publishedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".published");
        var consumedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".consumed");

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, 2);

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

        IReadOnlyList<CollectedMeasurement<int>> consumedMeasurements = consumedCollector.GetMeasurementSnapshot();
        Assert.NotEmpty(consumedMeasurements);
        Assert.Equal(1, consumedMeasurements[0].Value);
        /*
         * TODO - restore tags?
        Assert.Equal(consumedMeasurements[0].Tags["messaging.system"], "rabbitmq");
        Assert.Equal(consumedMeasurements[0].Tags["messaging.operation.name"], "deliver");
        Assert.Equal(consumedMeasurements[0].Tags["messaging.operation.type"], "process");
        Assert.Equal(consumedMeasurements[0].Tags["messaging.destination.name"], $"/queues/{Utils.EncodePathSegment(queueSpec.QueueName)}");
        Assert.Equal(consumedMeasurements[0].Tags["server.port"],
            _connectionSettings!.Port);
        Assert.Equal(consumedMeasurements[0].Tags["server.address"],
            _connectionSettings!.Host);
        */

        /*
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
        */

        await consumer.CloseAsync();
        consumer.Dispose();
    }
}
