// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class QueueSpec
    {
        private readonly IQueueSpecification _queueSpecification;

        internal QueueSpec(IQueueSpecification queueSpecification)
        {
            _queueSpecification = queueSpecification;
        }

        internal string QueueName => _queueSpecification.QueueName;

        internal bool IsExclusive => _queueSpecification.IsExclusive;

        internal bool IsAutoDelete => _queueSpecification.IsAutoDelete;

        internal Dictionary<object, object> QueueArguments => _queueSpecification.QueueArguments;
    }
}
