// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client
{
    public class FeatureFlags
    {

        /// <summary>
        /// Check if filter feature is enabled.
        /// Filter feature is available in RabbitMQ 4.1 and later.
        /// </summary>
        public bool IsFilterFeatureEnabled { get; internal set; } = false;

        /// <summary>
        /// Check if Sql feature is enabled.
        /// Sql feature is available in RabbitMQ 4.2 and later.
        /// </summary>
        public bool IsSqlFeatureEnabled { get; internal set; } = false;
        /// <summary>
        /// Check if the client is compatible with the broker version.
        /// The client requires RabbitMQ 4.0 or later to be compatible.
        /// </summary>
        public bool IsBrokerCompatible { get; internal set; } = false;

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
