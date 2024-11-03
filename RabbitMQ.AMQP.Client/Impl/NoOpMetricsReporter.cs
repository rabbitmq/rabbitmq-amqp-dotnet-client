using System;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class NoOpMetricsReporter : IMetricsReporter
    {
        public void ReportMessageSendSuccess(IMetricsReporter.PublisherContext context, long startTimestamp)
        {
        }

        public void ReportMessageSendFailure(IMetricsReporter.PublisherContext context, long startTimestamp,
            AmqpException amqpException)
        {
        }

        public void ReportMessageDeliverSuccess(IMetricsReporter.ConsumerContext context, long startTimestamp)
        {
        }

        public void ReportMessageDeliverFailure(IMetricsReporter.ConsumerContext consumerContext, long startTimestamp,
            Exception exception)
        {
        }
    }
}
