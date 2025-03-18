// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// Exception returned during consumer operations. See <see cref="IConsumer"/>.
    /// </summary>
    public class ConsumerException : Exception
    {
        public ConsumerException(string message) : base(message)
        {
        }
    }
}
