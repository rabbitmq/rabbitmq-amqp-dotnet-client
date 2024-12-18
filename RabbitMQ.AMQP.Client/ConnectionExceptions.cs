// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// Exception related to <see cref="IConnection"/>
    /// </summary>
    public class ConnectionException : Exception
    {
        /// <summary>
        /// Create a <see cref="ConnectionException"/> with the specified message.
        /// </summary>
        public ConnectionException(string message) : base(message)
        {
        }

        /// <summary>
        /// Create a <see cref="ConnectionException"/> with the specified message and inner <see cref="Exception"/>.
        /// </summary>
        public ConnectionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
