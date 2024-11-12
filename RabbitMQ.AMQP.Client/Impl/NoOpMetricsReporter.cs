// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class NoOpMetricsReporter : IMetricsReporter
    {
        public void ReportMessageSendSuccess(IMetricsReporter.PublisherContext context, TimeSpan elapsed)
        {
        }

        public void ReportMessageSendFailure(IMetricsReporter.PublisherContext context, TimeSpan elapsed,
            AmqpException amqpException)
        {
        }

        public void ReportMessageDeliverSuccess(IMetricsReporter.ConsumerContext context, TimeSpan elapsed)
        {
        }
    }
}
