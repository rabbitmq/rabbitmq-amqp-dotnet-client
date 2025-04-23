// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections;
using System.Collections.Generic;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class FieldNotSetException : Exception
    {
    }

    public class AmqpMessage : IMessage
    {
        public Message NativeMessage { get; }

        public AmqpMessage()
        {
            NativeMessage = new Message();
        }

        /// <summary>
        /// Create a message with a body of type byte[] and BodySection of type Data.
        /// </summary>
        /// <param name="body"></param>
        public AmqpMessage(byte[] body)
        {
            NativeMessage = new Message();
            NativeMessage.BodySection = new Data { Binary = body };
        }

        /// <summary>
        /// Create a message with a body of type string and BodySection of type Data.
        /// The string is converted to a byte[] using UTF8 encoding.
        /// </summary>
        public AmqpMessage(string body)
        {
            NativeMessage = new Message();
            NativeMessage.BodySection = new Data() { Binary = System.Text.Encoding.UTF8.GetBytes(body) };
        }

        public AmqpMessage(Message nativeMessage)
        {
            NativeMessage = nativeMessage;
        }

        public object MessageId()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.GetMessageId();
        }

        public IMessage MessageId(object id)
        {
            EnsureProperties();
            NativeMessage.Properties.SetMessageId(id);
            return this;
        }

        public byte[] UserId()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.UserId;
        }

        public IMessage UserId(byte[] userId)
        {
            EnsureProperties();
            NativeMessage.Properties.UserId = userId;
            return this;
        }

        public string To()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.To;
        }

        public IMessage To(string id)
        {
            EnsureProperties();
            NativeMessage.Properties.To = id;
            return this;
        }

        public string Subject()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.Subject;
        }

        public IMessage Subject(string subject)
        {
            EnsureProperties();
            NativeMessage.Properties.Subject = subject;
            return this;
        }

        public string ReplyTo()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.ReplyTo;
        }

        public IMessage ReplyTo(string id)
        {
            EnsureProperties();
            NativeMessage.Properties.ReplyTo = id;
            return this;
        }

        public object CorrelationId()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.GetCorrelationId();
        }

        public IMessage CorrelationId(object id)
        {
            EnsureProperties();
            NativeMessage.Properties.SetCorrelationId(id);
            return this;
        }

        public string ContentType()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.ContentType;
        }

        public IMessage ContentType(string contentType)
        {
            EnsureProperties();
            NativeMessage.Properties.ContentType = contentType;
            return this;
        }

        public string ContentEncoding()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.ContentEncoding;
        }

        public IMessage ContentEncoding(string contentEncoding)
        {
            EnsureProperties();
            NativeMessage.Properties.ContentEncoding = contentEncoding;
            return this;
        }

        public DateTime AbsoluteExpiryTime()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.AbsoluteExpiryTime;
        }

        public IMessage AbsoluteExpiryTime(DateTime absoluteExpiryTime)
        {
            EnsureProperties();
            NativeMessage.Properties.AbsoluteExpiryTime = absoluteExpiryTime;
            return this;
        }

        public DateTime CreationTime()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.CreationTime;
        }

        public IMessage CreationTime(DateTime creationTime)
        {
            EnsureProperties();
            NativeMessage.Properties.CreationTime = creationTime;
            return this;
        }

        public string GroupId()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.GroupId;
        }

        public IMessage GroupId(string groupId)
        {
            EnsureProperties();
            NativeMessage.Properties.GroupId = groupId;
            return this;
        }

        public uint GroupSequence()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.GroupSequence;
        }

        public IMessage GroupSequence(uint groupSequence)
        {
            EnsureProperties();
            NativeMessage.Properties.GroupSequence = groupSequence;
            return this;
        }

        public string ReplyToGroupId()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.ReplyToGroupId;
        }

        public IMessage ReplyToGroupId(string replyToGroupId)
        {
            EnsureProperties();
            NativeMessage.Properties.ReplyToGroupId = replyToGroupId;
            return this;
        }

        public IMessage Property(string key, object value)
        {
            EnsureApplicationProperties();
            NativeMessage.ApplicationProperties[key] = value;
            return this;
        }

        public IMessage PropertySymbol(string key, string value)
        {
            EnsureApplicationProperties();
            NativeMessage.ApplicationProperties[key] = new Symbol(value);
            return this;
        }

        public object Property(string key)
        {
            ThrowIfApplicationPropertiesNotSet();
            return NativeMessage.ApplicationProperties[key];
        }

        public IDictionary<object, object> Properties()
        {
            ThrowIfApplicationPropertiesNotSet();
            return NativeMessage.ApplicationProperties.Map;
        }

        // Annotations
        public IMessage Annotation(string key, object value)
        {
            EnsureAnnotations();
            Utils.ValidateMessageAnnotationKey(key);
            NativeMessage.MessageAnnotations[new Symbol(key)] = value;
            return this;
        }

        public object Annotation(string key)
        {
            ThrowIfAnnotationsNotSet();
            Utils.ValidateMessageAnnotationKey(key);
            return NativeMessage.MessageAnnotations[new Symbol(key)];
        }

        public byte[] Body()
        {
            return (byte[])NativeMessage.Body;
        }

        public string BodyAsString()
        {
            if (NativeMessage.BodySection is Data data)
            {
                return System.Text.Encoding.UTF8.GetString(data.Binary);
            }
            else
            {
                throw new InvalidOperationException("Body is not an Application Data");
            }

        }

        public IMessage Body(object body)
        {
            RestrictedDescribed bodySection;
            if (body is byte[] byteArray)
            {
                bodySection = new Data { Binary = byteArray };
            }
            else if (body is IList list)
            {
                bodySection = new AmqpSequence { List = list };
            }
            else
            {
                bodySection = new AmqpValue { Value = body };
            }

            NativeMessage.BodySection = bodySection;
            return this;
        }

        public IMessageAddressBuilder ToAddress()
        {
            return new MessageAddressBuilder(this);
        }

        private void ThrowIfPropertiesNotSet()
        {
            if (NativeMessage.Properties == null)
            {
                throw new FieldNotSetException();
            }
        }

        private void EnsureProperties()
        {
            NativeMessage.Properties ??= new Properties();
        }

        private void ThrowIfAnnotationsNotSet()
        {
            if (NativeMessage.MessageAnnotations == null)
            {
                throw new FieldNotSetException();
            }
        }

        private void EnsureAnnotations()
        {
            NativeMessage.MessageAnnotations ??= new MessageAnnotations();
        }

        private void ThrowIfApplicationPropertiesNotSet()
        {
            if (NativeMessage.ApplicationProperties == null)
            {
                throw new FieldNotSetException();
            }
        }

        private void EnsureApplicationProperties()
        {
            NativeMessage.ApplicationProperties ??= new ApplicationProperties();
        }
    }
}
