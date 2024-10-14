// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
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

        public AmqpMessage(object body)
        {
            NativeMessage = new Message(body);
        }

        public AmqpMessage(Message nativeMessage)
        {
            NativeMessage = nativeMessage;
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

        public object Body()
        {
            // TODO do we need to do anything with NativeMessage.BodySection?
            return NativeMessage.Body;
        }

        public object MessageId()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.GetMessageId();
        }

        public IMessage MessageId(string id)
        {
            EnsureProperties();
            NativeMessage.Properties.SetMessageId(id);
            return this;
        }

        public IMessage MessageId(object id)
        {
            EnsureProperties();
            NativeMessage.Properties.SetMessageId(id);
            return this;
        }

        public object CorrelationId()
        {
            ThrowIfPropertiesNotSet();
            return NativeMessage.Properties.CorrelationId;
        }

        public IMessage CorrelationId(string id)
        {
            EnsureProperties();
            NativeMessage.Properties.SetCorrelationId(id);
            return this;
        }

        public IMessage CorrelationId(object id)
        {
            EnsureProperties();
            NativeMessage.Properties.SetCorrelationId(id);
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

        // Annotations

        public IMessage Annotation(string key, object value)
        {
            EnsureAnnotations();
            NativeMessage.MessageAnnotations[new Symbol(key)] = value;
            return this;
        }

        public object Annotation(string key)
        {
            ThrowIfAnnotationsNotSet();
            return NativeMessage.MessageAnnotations[new Symbol(key)];
        }

        public IMessageAddressBuilder ToAddress()
        {
            return new MessageAddressBuilder(this);
        }
    }
}
