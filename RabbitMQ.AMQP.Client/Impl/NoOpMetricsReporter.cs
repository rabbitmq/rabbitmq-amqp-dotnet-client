using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class NoOpMetricsReporter : IMetricsReporter
    {
        public void ReportMessageSendSuccess(IMetricsReporter.PublisherContext context)
        {
        }

        public void ReportMessageSendFailure(IMetricsReporter.PublisherContext context, AmqpException amqpException)
        {
        }
    }
}
