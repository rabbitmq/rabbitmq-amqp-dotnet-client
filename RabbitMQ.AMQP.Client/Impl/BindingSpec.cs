// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class BindingSpec
    {
        private readonly IBindingSpecification _bindingSpecification;

        internal BindingSpec(IBindingSpecification bindingSpecification)
        {
            _bindingSpecification = bindingSpecification;
        }

        internal string SourceExchangeName => _bindingSpecification.SourceExchangeName;

        internal string DestinationExchangeName => _bindingSpecification.DestinationExchangeName;

        internal string DestinationQueueName => _bindingSpecification.DestinationQueueName;

        internal string BindingKey => _bindingSpecification.BindingKey;

        internal Dictionary<string, object> BindingArguments => _bindingSpecification.BindingArguments;

        internal string BindingPath => _bindingSpecification.BindingPath;
    }
}
