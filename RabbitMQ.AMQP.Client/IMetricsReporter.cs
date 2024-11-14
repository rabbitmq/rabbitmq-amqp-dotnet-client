// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using Amqp;

namespace RabbitMQ.AMQP.Client
{
    public interface IMetricsReporter
    {
        void ReportMessageSendSuccess(Context context, TimeSpan elapsed);

        void ReportMessageSendFailure(Context context, TimeSpan elapsed, AmqpException amqpException);

        void ReportMessageDeliverSuccess(Context context, TimeSpan elapsed);

        sealed class Context
        {
            public Context(string? destination, string serverAddress, int serverPort)
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
