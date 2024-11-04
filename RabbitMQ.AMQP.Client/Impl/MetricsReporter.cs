#if NET6_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Amqp;
#endif

namespace RabbitMQ.AMQP.Client.Impl
{
#if NET6_0_OR_GREATER
    // .NET docs on metric instrumentation: https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation
    // OpenTelemetry semantic conventions for messaging metric: https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics
    internal sealed class MetricsReporter : IMetricsReporter
    {
        const string Version = "0.1.0";

        static readonly Meter Meter;

        static readonly Counter<int> MessagingClientSentMessages;
        static readonly Histogram<double> MessagingClientOperationDuration;

        static readonly Counter<int> MessagingClientConsumedMessages;
        static readonly Histogram<double> MessagingProcessDuration;

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
        private const string ServerAddress = "server.adress";
        private const string ServerPort = "server.port";

        private const string ProcessOperation = "process";
        private const string DeliverOperation = "deliver";
        private const string PublishOperation = "publish";
        private const string SendOperation = "send";
        private const string MessagingSystemValue = "rabbitmq";

        static MetricsReporter()
        {
            Meter = new("RabbitMQ.Amqp", Version);

            MessagingClientSentMessages = Meter.CreateCounter<int>(
                "messaging.client.sent.messages",
                unit: "{message}",
                description:
                "Number of messages producer attempted to send to the broker.");

            MessagingClientOperationDuration = Meter.CreateHistogram<double>(
                "messaging.client.operation.duration",
                unit: "s",
                description:
                "Duration of messaging operation initiated by a producer or consumer client.");

            MessagingClientConsumedMessages = Meter.CreateCounter<int>(
                "messaging.client.consumed.messages",
                unit: "{message}",
                description:
                "Number of messages that were delivered to the application. ");

            MessagingProcessDuration = Meter.CreateHistogram<double>(
                "messaging.process.duration",
                unit: "s",
                description:
                "Duration of processing operation. ");
        }

        public void ReportMessageSendSuccess(IMetricsReporter.PublisherContext context, long startTimestamp)
        {
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);

            MessagingClientSentMessages.Add(1, serverAddress, serverPort, destination, _messagingOperationSystemTag,
                _sendOperationType, _publishOperationName);
            if (startTimestamp > 0)
            {
#if NET7_0_OR_GREATER
            var duration = Stopwatch.GetElapsedTime(startTimestamp);
#else
                var duration =
                    new TimeSpan((long)((Stopwatch.GetTimestamp() - startTimestamp) * StopWatchTickFrequency));
#endif
                MessagingClientOperationDuration.Record(duration.TotalSeconds, serverAddress, serverPort, destination,
                    _messagingOperationSystemTag, _sendOperationType, _publishOperationName);
            }
        }

        public void ReportMessageSendFailure(IMetricsReporter.PublisherContext context, long startTimestamp,
            AmqpException amqpException)
        {
            var errorType = new KeyValuePair<string, object?>(ErrorType, amqpException.GetType().Name);
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);
            MessagingClientSentMessages.Add(1, errorType, serverAddress, serverPort, destination,
                _messagingOperationSystemTag, _sendOperationType, _publishOperationName);

            if (startTimestamp > 0)
            {
#if NET7_0_OR_GREATER
            var duration = Stopwatch.GetElapsedTime(startTimestamp);
#else
                var duration =
                    new TimeSpan((long)((Stopwatch.GetTimestamp() - startTimestamp) * StopWatchTickFrequency));
#endif
                MessagingClientOperationDuration.Record(duration.TotalSeconds, errorType, serverAddress, serverPort,
                    destination,
                    _messagingOperationSystemTag, _sendOperationType, _publishOperationName);
            }
        }

        public void ReportMessageDeliverSuccess(IMetricsReporter.ConsumerContext context, long startTimestamp)
        {
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);
            MessagingClientConsumedMessages.Add(1, serverAddress, serverPort, destination, _messagingOperationSystemTag,
                _processOperationType, _deliverOperationName);
            if (startTimestamp > 0)
            {
#if NET7_0_OR_GREATER
            var duration = Stopwatch.GetElapsedTime(startTimestamp);
#else
                var duration =
                    new TimeSpan((long)((Stopwatch.GetTimestamp() - startTimestamp) * StopWatchTickFrequency));
#endif
                MessagingProcessDuration.Record(duration.TotalSeconds, serverAddress, serverPort,
                    destination,
                    _messagingOperationSystemTag, _sendOperationType, _publishOperationName);
            }
        }

        public void ReportMessageDeliverFailure(IMetricsReporter.ConsumerContext context, long startTimestamp,
            Exception exception)
        {
            var errorType = new KeyValuePair<string, object?>(ErrorType, exception.GetType().Name);
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);
            MessagingClientConsumedMessages.Add(1, errorType, serverAddress, serverPort, destination,
                _messagingOperationSystemTag,
                _processOperationType, _deliverOperationName);
            if (startTimestamp > 0)
            {
#if NET7_0_OR_GREATER
            var duration = Stopwatch.GetElapsedTime(startTimestamp);
#else
                var duration =
                    new TimeSpan((long)((Stopwatch.GetTimestamp() - startTimestamp) * StopWatchTickFrequency));
#endif
                MessagingProcessDuration.Record(duration.TotalSeconds, errorType, serverAddress, serverPort,
                    destination,
                    _messagingOperationSystemTag, _sendOperationType, _publishOperationName);
            }
        }
#if !NET7_0_OR_GREATER
        const long TicksPerMicrosecond = 10;
        const long TicksPerMillisecond = TicksPerMicrosecond * 1000;
        const long TicksPerSecond = TicksPerMillisecond * 1000; // 10,000,000
        static readonly double StopWatchTickFrequency = (double)TicksPerSecond / Stopwatch.Frequency;
#endif
    }
#else
#endif
}
