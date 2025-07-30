// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client
{
    internal class FeatureFlags
    {
        public bool IsSqlFeatureEnabled { get; set; } = false;
        public bool IsBrokerCompatible { get; set; } = false;

        public void Validate()
        {
            if (!IsBrokerCompatible)
            {
                throw new ConnectionException("Client not compatible with the broker version. " +
                                              "The client requires RabbitMQ 4.0 or later.");
            }
        }
    }
}
