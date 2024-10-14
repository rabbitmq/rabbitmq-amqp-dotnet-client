// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client
{
    public interface IMessage
    {
        // TODO: Complete the IMessage interface with all the  properties

        // TODO does this depend on NativeMessage.BodySection?
        public object Body();

        // properties
        IMessage MessageId(string id);

        IMessage MessageId(object id);
        object MessageId();

        object CorrelationId();
        IMessage CorrelationId(string id);
        IMessage CorrelationId(object id);

        string ReplyTo();
        IMessage ReplyTo(string id);

        string To();
        IMessage To(string id);

        string Subject();
        IMessage Subject(string subject);

        public IMessage Annotation(string key, object value);

        public object Annotation(string key);

        IMessageAddressBuilder ToAddress();
    }
}
