// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public class PublisherException : Exception
    {
        public PublisherException(string message) : base(message)
        {
        }
    }

    /// <summary>
    ///  Represents the status of a publish operation.
    ///  Accepted: The message was accepted for publication.
    ///  Rejected: The message was rejected by the broker.
    ///  Released: The message was released by the broker.
    /// </summary>
    public enum OutcomeState
    {
        Accepted,
        Rejected,
        Released,
    }

    /// <summary>
    ///  PublishOutcome represents the outcome of a publish operation.
    ///  It contains the state of the outcome and an error if the outcome is not successful.
    /// </summary>

    public class PublishOutcome
    {
        public PublishOutcome(OutcomeState state, Error? error)
        {
            State = state;
            Error = error;
        }

        public OutcomeState State { get; }

        public Error? Error { get; }
    }

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
    ///  Interface for publishing messages to an AMQP broker.
    ///  Implementations of this interface are expected to be thread-safe.
    /// </summary>
    public interface IPublisher : ILifeCycle
    {
        Task<PublishResult> PublishAsync(IMessage message, CancellationToken cancellationToken = default);
    }
}
