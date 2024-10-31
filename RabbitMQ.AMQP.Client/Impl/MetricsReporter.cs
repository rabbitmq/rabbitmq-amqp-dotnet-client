using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
// .NET docs on metric instrumentation: https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation
// OpenTelemetry semantic conventions for messaging metric: https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics
    internal sealed class SystemDiagnosticMetricsReporter : IMetricsReporter
    {
        const string Version = "0.1.0";

        static readonly Meter Meter;

        static readonly Counter<int> MessagingClientSentMessages;

        readonly KeyValuePair<string, object?> _messagingOperationSystemTag = new(MessagingSystem, "rabbitmq");

        private const string MessagingSystem = "messaging.system";
        private const string ErrorType = "error.type";
        private const string MessageDestinationName = "messaging.destination.name";
        private const string ServerAddress = "server.adress";
        private const string ServerPort = "server.port";


        static SystemDiagnosticMetricsReporter()
        {
            Meter = new("RabbitMQ.Amqp", Version);

            MessagingClientSentMessages = Meter.CreateCounter<int>(
                "messaging.client.sent.messages",
                unit: "{message}",
                description:
                "Number of messages producer attempted to send to the broker.");
        }


        public void ReportMessageSendSuccess(IMetricsReporter.PublisherContext context)
        {
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);
            MessagingClientSentMessages.Add(1, serverAddress, serverPort, destination, _messagingOperationSystemTag);
        }


        public void ReportMessageSendFailure(IMetricsReporter.PublisherContext context, AmqpException amqpException)
        {
            var errorType = new KeyValuePair<string, object?>(ErrorType, amqpException.GetType().Name);
            var serverAddress = new KeyValuePair<string, object?>(ServerAddress, context.ServerAddress);
            var serverPort = new KeyValuePair<string, object?>(ServerPort, context.ServerPort);
            var destination = new KeyValuePair<string, object?>(MessageDestinationName, context.Destination);
            MessagingClientSentMessages.Add(1, errorType, serverAddress, serverPort, destination,
                _messagingOperationSystemTag);
        }
    }
}
