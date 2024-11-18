// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client
{
    public class InternalBugException : InvalidOperationException
    {
        public InternalBugException() : base()
        {
        }

        public InternalBugException(string message) : base(message)
        {
        }

        public static void CreateAndThrow(string message)
        {
            throw new InternalBugException(message +
                ", report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
        }
    }
}
