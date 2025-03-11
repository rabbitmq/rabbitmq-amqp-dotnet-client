// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class ExchangeSpec
    {
        private readonly IExchangeSpecification _exchangeSpecification;

        internal ExchangeSpec(IExchangeSpecification exchangeSpecification)
        {
            _exchangeSpecification = exchangeSpecification;
        }

        internal string ExchangeName => _exchangeSpecification.ExchangeName;

        internal string ExchangeType => _exchangeSpecification.ExchangeType;

        internal bool IsAutoDelete => _exchangeSpecification.IsAutoDelete;

        internal Dictionary<string, object> ExchangeArguments => _exchangeSpecification.ExchangeArguments;
    }
}
