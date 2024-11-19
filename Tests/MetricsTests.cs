// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class MetricsTests : IntegrationTest, IMeterFactory
{
    private const string MetricPrefix = MetricsReporter.MetricPrefix;
    private readonly MetricsReporter _metricsReporter;
    private Meter? _meter;

    public MetricsTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, setupConnectionAndManagement: false)
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
        const int messageCount = 100;

        var connectionsCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".connections");
        var publishersCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".publishers");
        var consumersCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".consumers");

        var publishedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".published");
        var publishDurationCollector =
            new MetricCollector<double>(this, MetricsReporter.MeterName, MetricPrefix + ".published.duration");
        var publishedAcceptedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".published_accepted");
        var publishedRejectedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".published_rejected");
        var publishedReleasedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".published_released");

        var consumedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".consumed");
        var consumeDurationCollector =
            new MetricCollector<double>(this, MetricsReporter.MeterName, MetricPrefix + ".consumed.duration");
        var consumedAcceptedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".consumed_accepted");
        var consumedRequeuedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".consumed_requeued");
        var consumedDiscardedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".consumed_discarded");

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        await PublishAsync(queueSpec, messageCount);

        int receivedCount = 0;
        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueSpec)
            .MessageHandler((context, message) =>
                {
                    try
                    {
                        context.Accept();
                        if (Interlocked.Increment(ref receivedCount) == messageCount)
                        {
                            tcs.SetResult(true);
                        }
                    }
                    catch (Exception ex)
                    {
                        tcs.SetException(ex);
                    }
                    return Task.CompletedTask;
                }
            ).BuildAndStartAsync();

        await WhenTcsCompletes(tcs);

        IReadOnlyList<CollectedMeasurement<int>> publishedMeasurements = publishedCollector.GetMeasurementSnapshot();
        Assert.Equal(messageCount, publishedMeasurements.Count);
        Assert.Equal(1, publishedMeasurements[0].Value);

        IReadOnlyList<CollectedMeasurement<int>> publishedAcceptedMeasurements = publishedAcceptedCollector.GetMeasurementSnapshot();
        Assert.Equal(messageCount, publishedAcceptedMeasurements.Count);
        Assert.Equal(1, publishedAcceptedMeasurements[0].Value);

        IReadOnlyList<CollectedMeasurement<int>> publishedRejectedMeasurements = publishedRejectedCollector.GetMeasurementSnapshot();
        Assert.Empty(publishedRejectedMeasurements);

        IReadOnlyList<CollectedMeasurement<int>> publishedReleasedMeasurements = publishedReleasedCollector.GetMeasurementSnapshot();
        Assert.Empty(publishedReleasedMeasurements);

        IReadOnlyList<CollectedMeasurement<double>> publishDurationMeasurements = publishDurationCollector.GetMeasurementSnapshot();
        Assert.NotEmpty(publishDurationMeasurements);
        Assert.True(publishDurationMeasurements[0].Value > 0);

        IReadOnlyList<CollectedMeasurement<int>> consumedMeasurements = consumedCollector.GetMeasurementSnapshot();
        Assert.Equal(messageCount, consumedMeasurements.Count);
        Assert.Equal(1, consumedMeasurements[0].Value);

        IReadOnlyList<CollectedMeasurement<int>> consumedAcceptedMeasurements = consumedAcceptedCollector.GetMeasurementSnapshot();
        Assert.Equal(messageCount, consumedAcceptedMeasurements.Count);
        Assert.Equal(1, consumedAcceptedMeasurements[0].Value);

        IReadOnlyList<CollectedMeasurement<int>> consumedRequeuedMeasurements = consumedRequeuedCollector.GetMeasurementSnapshot();
        Assert.Empty(consumedRequeuedMeasurements);

        IReadOnlyList<CollectedMeasurement<int>> consumedDiscardedMeasurements = consumedDiscardedCollector.GetMeasurementSnapshot();
        Assert.Empty(consumedDiscardedMeasurements);

        IReadOnlyList<CollectedMeasurement<double>> consumeDurationMeasurements = consumeDurationCollector.GetMeasurementSnapshot();
        Assert.NotEmpty(consumeDurationMeasurements);
        Assert.True(consumeDurationMeasurements[0].Value > 0);
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

    [Fact]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Assertions",
        "xUnit2013:Do not use equality check to check for collection size.",
        Justification = "messageCount may change")]
    public async Task PublisherMetricsShouldBeIncrementedWhenMessageIsSendWithSuccess()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 1;

        var publishedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".published");
        var publishDurationCollector =
            new MetricCollector<double>(this, MetricsReporter.MeterName, MetricPrefix + ".published.duration");

        IQueueSpecification queueSpecification = _management.Queue(_queueName);
        await queueSpecification.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder()
            .Queue(queueSpecification)
            .BuildAsync();

        await PublishAsync(queueSpecification, messageCount);

        IReadOnlyList<CollectedMeasurement<int>> publishedMeasurements = publishedCollector.GetMeasurementSnapshot();
        Assert.Equal(messageCount, publishedMeasurements.Count);
        Assert.Equal(1, publishedMeasurements[0].Value);

        IReadOnlyList<CollectedMeasurement<double>> publishDurationMeasurements = publishDurationCollector.GetMeasurementSnapshot();
        Assert.Equal(messageCount, publishDurationMeasurements.Count);
        Assert.True(publishDurationMeasurements[0].Value > 0);

        /*
         * TODO - restore tags?
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["messaging.system"], "rabbitmq");
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["messaging.operation.name"], "publish");
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["messaging.operation.type"], "send");
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["messaging.destination.name"],
            $"/queues/{Utils.EncodePathSegment(queueSpecification.QueueName)}");
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["server.port"],
            _connectionSettings!.Port);
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["server.address"],
            _connectionSettings!.Host);
        */

        /*
        var clientSendDurationsMeasurements =
            clientSendDurationCollector
                .GetMeasurementSnapshot();
        Assert.NotEmpty(clientSendDurationsMeasurements);
        Assert.True(clientSendDurationsMeasurements[0].Value > 0);
        Assert.Equal(clientSendDurationsMeasurements[0].Tags["messaging.system"], "rabbitmq");
        Assert.Equal(clientSendDurationsMeasurements[0].Tags["messaging.operation.name"], "publish");
        Assert.Equal(clientSendDurationsMeasurements[0].Tags["messaging.operation.type"], "send");
        Assert.Equal(clientSendDurationsMeasurements[0].Tags["messaging.destination.name"],
            $"/queues/{Utils.EncodePathSegment(queueSpecification.QueueName)}");
        Assert.Equal(clientSendDurationsMeasurements[0].Tags["server.port"],
            _connectionSettings!.Port);
        Assert.Equal(clientSendDurationsMeasurements[0].Tags["server.address"],
            _connectionSettings!.Host);
        */

        await publisher.CloseAsync();
        publisher.Dispose();
    }

    [Fact]
    public async Task PublisherShouldRecordAFailureWhenSendingThrowAnException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        var publishedCollector =
            new MetricCollector<int>(this, MetricsReporter.MeterName, MetricPrefix + ".published");
        /*
        MetricCollector<double> clientSendDurationCollector =
            new(_meterFactory, "RabbitMQ.Amqp", "messaging.client.operation.duration");
        */

        IMessage message = new AmqpMessage(RandomBytes());

        IExchangeSpecification exchangeSpecification = _management.Exchange(_exchangeName).Type(ExchangeType.FANOUT);
        await exchangeSpecification.DeclareAsync();

        IPublisherBuilder publisherBuilder = _connection.PublisherBuilder();
        // TODO implement Listeners
        IPublisher publisher = await publisherBuilder.Exchange(exchangeSpecification).BuildAsync();

        try
        {
            IQueueSpecification queueSpecification = _management.Queue().Exclusive(true);
            IQueueInfo queueInfo = await queueSpecification.DeclareAsync();
            IBindingSpecification bindingSpecification = _management.Binding()
                .SourceExchange(_exchangeName)
                .DestinationQueue(queueInfo.Name());
            await bindingSpecification.BindAsync();

            PublishResult publishResult = await publisher.PublishAsync(message);
            Assert.Equal(OutcomeState.Accepted, publishResult.Outcome.State);
        }
        finally
        {
            await exchangeSpecification.DeleteAsync();
        }

        for (int i = 0; i < 100; i++)
        {
            PublishResult nextPublishResult = await publisher.PublishAsync(message);
            if (OutcomeState.Rejected == nextPublishResult.Outcome.State)
            {
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        CollectedMeasurement<int> failedSendMeasure = publishedCollector.LastMeasurement!;
        Assert.Equal(1, failedSendMeasure.Value);

        /*
         * TODO restore tags
        Assert.Equal(failedSendMeasure.Tags["messaging.system"], "rabbitmq");
        Assert.Equal(failedSendMeasure.Tags["messaging.operation.name"], "publish");
        Assert.Equal(failedSendMeasure.Tags["messaging.operation.type"], "send");
        Assert.Equal(failedSendMeasure.Tags["messaging.destination.name"],
            $"/exchanges/{Utils.EncodePathSegment(exchangeSpecification.ExchangeName)}");
        Assert.Equal(failedSendMeasure.Tags["server.port"],
            _connectionSettings!.Port);
        Assert.Equal(failedSendMeasure.Tags["server.address"],
            _connectionSettings!.Host);
        Assert.Equal(failedSendMeasure.Tags["error.type"],
            "amqp:not-found");
        */

        /*
         * TODO restore durations
        var failedMessageSendDuration =
            clientSendDurationCollector
                .LastMeasurement!;
        Assert.True(failedMessageSendDuration.Value > 0);
        Assert.Equal(failedMessageSendDuration.Tags["messaging.system"], "rabbitmq");
        Assert.Equal(failedMessageSendDuration.Tags["messaging.operation.name"], "publish");
        Assert.Equal(failedMessageSendDuration.Tags["messaging.operation.type"], "send");
        Assert.Equal(failedMessageSendDuration.Tags["messaging.destination.name"],
            $"/exchanges/{Utils.EncodePathSegment(exchangeSpecification.ExchangeName)}");
        Assert.Equal(failedMessageSendDuration.Tags["server.port"],
            _connectionSettings!.Port);
        Assert.Equal(failedMessageSendDuration.Tags["server.address"],
            _connectionSettings!.Host);
        Assert.Equal(failedMessageSendDuration.Tags["error.type"],
            "amqp:not-found");
        */

        await publisher.CloseAsync();
        publisher.Dispose();
    }
}
