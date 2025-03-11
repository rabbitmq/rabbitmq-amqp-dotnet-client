// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// Thrown when management response code or correlation ID mismatch.
    /// </summary>
    public class ModelException : Exception
    {
        /// <summary>
        /// Create a new <see cref="ModelException"/>
        /// </summary>
        /// <param name="message">The exception message.</param>
        public ModelException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// Thrown when management response is "precondition failed".
    /// </summary>
    public class PreconditionFailedException : Exception
    {
        /// <summary>
        /// Create a new <see cref="PreconditionFailedException"/>
        /// </summary>
        /// <param name="message">The exception message.</param>
        public PreconditionFailedException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// Thrown when management response is code 400, bad request.
    /// </summary>
    public class BadRequestException : Exception
    {
        /// <summary>
        /// Create a new <see cref="BadRequestException"/>
        /// </summary>
        /// <param name="message">The exception message.</param>
        public BadRequestException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// Thrown when management response code is not expected.
    /// </summary>
    public class InvalidCodeException : Exception
    {
        /// <summary>
        /// Create a new <see cref="InvalidCodeException"/>
        /// </summary>
        /// <param name="message">The exception message.</param>
        public InvalidCodeException(string message) : base(message)
        {
        }
    }
}
