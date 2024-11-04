// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

#if NET8_0_OR_GREATER
using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Publisher;

public class PublisherMetricsTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task PublisherMetricsShouldBeIncrementedWhenMessageIsSendWithSuccess()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        Assert.NotNull(_meterFactory);
        Assert.NotNull(_metricsReporter);
        MetricCollector<int> clientSendMessageCounterCollector =
            new(_meterFactory, "RabbitMQ.Amqp", "messaging.client.sent.messages");
        MetricCollector<double> clientSendDurationCollector =
            new(_meterFactory, "RabbitMQ.Amqp", "messaging.client.operation.duration");

        IQueueSpecification queueSpecification = _management.Queue(_queueName);
        await queueSpecification.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpecification).BuildAsync();

        await publisher.PublishAsync(new AmqpMessage("Hello wold!"));

        await SystemUtils.WaitUntilQueueMessageCount(queueSpecification, 1);

        var clientSendMessagesMeasurements =
            clientSendMessageCounterCollector
                .GetMeasurementSnapshot();
        Assert.NotEmpty(clientSendMessagesMeasurements);
        Assert.Equal(1, clientSendMessagesMeasurements[0].Value);
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["messaging.system"], "rabbitmq");
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["messaging.operation.name"], "publish");
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["messaging.operation.type"], "send");
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["messaging.destination.name"],
            $"/queues/{Utils.EncodePathSegment(queueSpecification.QueueName)}");
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["server.port"],
            _connectionSettings!.Port);
        Assert.Equal(clientSendMessagesMeasurements[0].Tags["server.address"],
            _connectionSettings!.Host);

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

        await publisher.CloseAsync();
        publisher.Dispose();
    }

    [Fact]
    public async Task PublisherShouldRecordAFailureWhenSendingThrowAnException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        Assert.NotNull(_meterFactory);
        Assert.NotNull(_metricsReporter);
        MetricCollector<int> clientSendMessageCounterCollector =
            new(_meterFactory, "RabbitMQ.Amqp", "messaging.client.sent.messages");
        MetricCollector<double> clientSendDurationCollector =
            new(_meterFactory, "RabbitMQ.Amqp", "messaging.client.operation.duration");
        
        IMessage message = new AmqpMessage(Encoding.ASCII.GetBytes("hello"));

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

        var failedSendMeasure =
            clientSendMessageCounterCollector
                .LastMeasurement!;
        Assert.Equal(1, failedSendMeasure.Value);
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
        await publisher.CloseAsync();
        publisher.Dispose();
    }
}
#endif
