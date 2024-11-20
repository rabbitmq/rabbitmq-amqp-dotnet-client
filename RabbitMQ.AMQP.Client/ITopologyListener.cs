// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client
{
    internal interface ITopologyListener
    {
        void QueueDeclared(IQueueSpecification specification);

        void QueueDeleted(string name);

        void ExchangeDeclared(IExchangeSpecification specification);

        void ExchangeDeleted(string name);

        void BindingDeclared(IBindingSpecification specification);

        void BindingDeleted(string path);

        void Clear();

        int QueueCount();

        int ExchangeCount();

        int BindingCount();
    }
}
