using System;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    public interface IMetricsReporter
    {
        void ReportMessageSendSuccess(PublisherContext context, long startTimestamp);
        void ReportMessageSendFailure(PublisherContext context, long startTimestamp, AmqpException amqpException);
        public void ReportMessageDeliverSuccess(ConsumerContext context, long startTimestamp);
        void ReportMessageDeliverFailure(ConsumerContext consumerContext, long startTimestamp, Exception exception);
        sealed class ConsumerContext
        {
            public ConsumerContext(string? destination, string serverAddress, int serverPort)
            {
                Destination = destination;
                ServerAddress = serverAddress;
                ServerPort = serverPort;
            }

            public string? Destination { get; }
            public string ServerAddress { get; }
            public int ServerPort { get; }
        }
        
        sealed class PublisherContext
        {
            public PublisherContext(string? destination, string serverAddress, int serverPort)
            {
                Destination = destination;
                ServerAddress = serverAddress;
                ServerPort = serverPort;
            }

            public string? Destination { get; }
            public string ServerAddress { get; }
            public int ServerPort { get; }
        }
    }
}
