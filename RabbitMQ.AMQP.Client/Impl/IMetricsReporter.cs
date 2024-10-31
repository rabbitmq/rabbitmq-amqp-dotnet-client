using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    public interface IMetricsReporter
    {
        void ReportMessageSendSuccess(PublisherContext context);
        void ReportMessageSendFailure(PublisherContext context, AmqpException amqpException);

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
