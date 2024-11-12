// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    // .NET docs on metric instrumentation: https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation
    // OpenTelemetry semantic conventions for messaging metric: https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics
    internal sealed class MetricsReporter : IMetricsReporter
    {
        const string Version = "0.1.0";

        readonly Counter<int> _messagingClientSentMessages;
        readonly Histogram<double> _messagingClientOperationDuration;

        readonly Counter<int> _messagingClientConsumedMessages;
        readonly Histogram<double> _messagingProcessDuration;

        readonly KeyValuePair<string, object?>
            _messagingOperationSystemTag = new(MessagingSystem, MessagingSystemValue);

        readonly KeyValuePair<string, object?> _publishOperationName = new(MessagingOperationName, PublishOperation);
        readonly KeyValuePair<string, object?> _sendOperationType = new(MessagingOperationType, SendOperation);

        readonly KeyValuePair<string, object?> _deliverOperationName = new(MessagingOperationName, DeliverOperation);
        readonly KeyValuePair<string, object?> _processOperationType = new(MessagingOperationType, ProcessOperation);

        private const string MessagingOperationName = "messaging.operation.name";
        private const string MessagingOperationType = "messaging.operation.type";
        private const string MessagingSystem = "messaging.system";
        private const string ErrorType = "error.type";
        private const string MessageDestinationName = "messaging.destination.name";
        private const string ServerAddress = "server.address";
        private const string ServerPort = "server.port";

        private const string ProcessOperation = "process";
        private const string DeliverOperation = "deliver";
        private const string PublishOperation = "publish";
        private const string SendOperation = "send";
        private const string MessagingSystemValue = "rabbitmq";

        private const string DefaultErrorValue = "_OTHER";
        public MetricsReporter(IMeterFactory meterFactory)
        {
            Meter meter = meterFactory.Create("RabbitMQ.Amqp", Version);

            _messagingClientSentMessages = meter.CreateCounter<int>(
                "messaging.client.sent.messages",
                unit: "{message}",
                description:
                "Number of messages producer attempted to send to the broker.");

            _messagingClientOperationDuration = meter.CreateHistogram<double>(
                "messaging.client.operation.duration",
                unit: "s",
                description:
                "Duration of messaging operation initiated by a producer or consumer client.");

            _messagingClientConsumedMessages = meter.CreateCounter<int>(
                "messaging.client.consumed.messages",
                unit: "{message}",
                description:
                "Number of messages that were delivered to the application. ");

            _messagingProcessDuration = meter.CreateHistogram<double>(
                "messaging.process.duration",
                unit: "s",
                description:
                "Duration of processing operation. ");
        }

        public void ReportMessageSendSuccess(IMetricsReporter.PublisherContext context, TimeSpan elapsed)
        {
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);

            _messagingClientSentMessages.Add(1, serverAddress, serverPort, destination, _messagingOperationSystemTag,
                _sendOperationType, _publishOperationName);

            if (elapsed != default)
            {
                _messagingClientOperationDuration.Record(elapsed.TotalMilliseconds, serverAddress, serverPort, destination,
                    _messagingOperationSystemTag, _sendOperationType, _publishOperationName);
            }
        }

        public void ReportMessageSendFailure(IMetricsReporter.PublisherContext context, TimeSpan elapsed,
            AmqpException amqpException)
        {
            var errorType = new KeyValuePair<string, object?>(ErrorType, amqpException.Error.Condition.ToString() ?? DefaultErrorValue);
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);
            _messagingClientSentMessages.Add(1, errorType, serverAddress, serverPort, destination,
                _messagingOperationSystemTag, _sendOperationType, _publishOperationName);

            if (elapsed != default)
            {
                _messagingClientOperationDuration.Record(elapsed.TotalMilliseconds, errorType, serverAddress, serverPort,
                    destination,
                    _messagingOperationSystemTag, _sendOperationType, _publishOperationName);
            }
        }

        public void ReportMessageDeliverSuccess(IMetricsReporter.ConsumerContext context, TimeSpan elapsed)
        {
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);

            _messagingClientConsumedMessages.Add(1, serverAddress, serverPort, destination,
                _messagingOperationSystemTag,
                _processOperationType, _deliverOperationName);

            if (elapsed != default)
            {
                _messagingProcessDuration.Record(elapsed.TotalMilliseconds, serverAddress, serverPort,
                    destination,
                    _messagingOperationSystemTag, _processOperationType, _deliverOperationName);
            }
        }
    }
}
