// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client
{
    public class PublisherException : Exception
    {
        public PublisherException(string message) : base(message)
        {
        }

        public PublisherException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
