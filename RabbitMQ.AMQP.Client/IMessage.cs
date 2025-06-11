// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;

namespace RabbitMQ.AMQP.Client
{
    public interface IMessage
    {
        // Immutable properties of the message (azure/amqpnetlite/src/Framing/Properties.cs)

        /// <summary>
        /// The message id must be of <see cref="T:byte[]" />, <see cref="Guid"/>,
        /// <see cref="string"/> or <see cref="ulong"/>.
        /// </summary>
        /// <returns>The message id as <see cref="object"/>, which can be cast to <see cref="T:byte[]" />,
        /// <see cref="Guid"/>, <see cref="string"/> or <see cref="ulong"/>.</returns>
        object MessageId();

        /// <summary>
        /// The message id must be of <see cref="T:byte[]" />, <see cref="Guid"/>,
        /// <see cref="string"/> or <see cref="ulong"/>.
        /// </summary>
        /// <param name="messageId"></param>
        /// <returns>This message instance.</returns>
        IMessage MessageId(object messageId);

        /// <summary>
        /// The user id
        /// </summary>
        /// <returns>The user id as <see cref="T:byte[]"/>".</returns>
        byte[] UserId();

        /// <summary>
        /// Set the message user id
        /// </summary>
        /// <param name="userId"></param>
        /// <returns>This message instance.</returns>
        IMessage UserId(byte[] userId);

        string To();
        IMessage To(string to);

        string Subject();
        IMessage Subject(string subject);

        string ReplyTo();
        IMessage ReplyTo(string replyTo);

        /// <summary>
        /// The correlation id must be of <see cref="T:byte[]" />, <see cref="Guid"/>,
        /// <see cref="string"/> or <see cref="ulong"/>.
        /// </summary>
        /// <returns>The correlation id as <see cref="object"/>, which can be cast to <see cref="T:byte[]" />,
        /// <see cref="Guid"/>, <see cref="string"/> or <see cref="ulong"/>.</returns>
        object CorrelationId();

        /// <summary>
        /// The correlation id must be of <see cref="T:byte[]" />, <see cref="Guid"/>,
        /// <see cref="string"/> or <see cref="ulong"/>.
        /// </summary>
        /// <param name="id"></param>
        /// <returns>This message instance.</returns>
        IMessage CorrelationId(object id);

        string ContentType();
        IMessage ContentType(string contentType);

        string ContentEncoding();
        IMessage ContentEncoding(string contentEncoding);

        DateTime AbsoluteExpiryTime();
        IMessage AbsoluteExpiryTime(DateTime absoluteExpiryTime);

        DateTime CreationTime();
        IMessage CreationTime(DateTime creationTime);

        string GroupId();
        IMessage GroupId(string groupId);

        uint GroupSequence();
        IMessage GroupSequence(uint groupSequence);

        string ReplyToGroupId();
        IMessage ReplyToGroupId(string replyToGroupId);

        // Application properties of the message (azure/amqpnetlite/src/Framing/ApplicationProperties.cs)
        public object Property(string key);
        public IMessage Property(string key, object value);
        public IMessage PropertySymbol(string key, string value);
        public IDictionary<object, object> Properties();

        // Message annotations
        public object Annotation(string key);
        public IMessage Annotation(string key, object value);

        public byte[] Body();

        public string BodyAsString();

        public IMessage Body(object body);

        public IMessage Durable(bool durable);

        public bool Durable();

        IMessageAddressBuilder ToAddress();
    }
}
