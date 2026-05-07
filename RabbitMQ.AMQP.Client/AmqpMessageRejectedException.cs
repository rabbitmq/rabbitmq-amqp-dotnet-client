// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// Exception thrown when a message is rejected by the broker.
    /// Available when a <see cref="PublishOutcome"/> has state <see cref="OutcomeState.Rejected"/>
    /// and the broker provided rejection details (requires RabbitMQ 4.3 or later).
    /// </summary>
    public class AmqpMessageRejectedException : Exception
    {
        public AmqpMessageRejectedException(string? message, string? rejectedBy, string? reason) : base(message)
        {
            RejectedBy = rejectedBy;
            Reason = reason;
        }

        /// <summary>
        /// The name of the queue that rejected the message, if provided by the broker.
        /// </summary>
        public string? RejectedBy { get; }

        public string? Reason { get; }
    }
}
