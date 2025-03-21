// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    ///  Represents the status of a publish operation.
    ///  See <see href="https://www.rabbitmq.com/docs/amqp#outcomes">AMQP Outcomes</see>.
    /// </summary>
    public enum OutcomeState
    {
        /// <summary>
        /// The message has been accepted by the broker.
        /// </summary>
        Accepted,

        /// <summary>
        /// At least one queue the message was routed to rejected the message. This happens when the
        /// queue length is exceeded and the queue's overflow behaviour is set to reject-publish or when
        /// a target classic queue is unavailable.
        /// </summary>
        Rejected,

        /// <summary>
        /// The broker could not route the message to any queue.
        /// This is likely to be due to a topology misconfiguration.
        /// </summary>
        Released
    }

    /// <summary>
    /// Represents the outcome of a publish operation.
    /// It contains the state of the outcome and an error if the outcome is not successful.
    /// </summary>
    public class PublishOutcome
    {
        public PublishOutcome(OutcomeState state, Error? error)
        {
            State = state;
            Error = error;
        }

        /// <summary>
        /// The <see cref="OutcomeState"/>.
        /// </summary>
        public OutcomeState State { get; }

        /// <summary>
        /// The <see cref="Error"/>, if any.
        /// </summary>
        public Error? Error { get; }
    }

    /// <summary>
    /// Represents the result of a publish operation.
    /// It contains the <see cref="PublishOutcome"/> and the original <see cref="IMessage"/>.
    /// </summary>
    public class PublishResult
    {
        public PublishResult(IMessage message, PublishOutcome outcome)
        {
            Message = message;
            Outcome = outcome;
        }

        public IMessage Message { get; }

        public PublishOutcome Outcome { get; }
    }

    /// <summary>
    /// Interface for publishing messages to an AMQP broker.
    /// Implementations of this interface are expected to be thread-safe.
    /// </summary>
    public interface IPublisher : ILifeCycle
    {
        Task<PublishResult> PublishAsync(IMessage message, CancellationToken cancellationToken = default);
    }
}
