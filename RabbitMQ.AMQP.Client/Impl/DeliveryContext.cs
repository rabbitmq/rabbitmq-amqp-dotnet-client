// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using Amqp;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class DeliveryContext : IContext
    {
        private readonly IReceiverLink _link;
        private readonly Message _message;
        private readonly UnsettledMessageCounter _unsettledMessageCounter;

        public DeliveryContext(IReceiverLink link, Message message, UnsettledMessageCounter unsettledMessageCounter)
        {
            _link = link;
            _message = message;
            _unsettledMessageCounter = unsettledMessageCounter;
        }

        public void Accept()
        {
            try
            {
                if (_link.IsClosed)
                {
                    throw new ConsumerException("Link is closed");
                }

                _link.Accept(_message);
                _unsettledMessageCounter.Decrement();
            }
            finally
            {
                _message.Dispose();
            }
        }

        public void Discard()
        {
            try
            {
                if (_link.IsClosed)
                {
                    throw new ConsumerException("Link is closed");
                }

                _link.Reject(_message);
                _unsettledMessageCounter.Decrement();
            }
            finally
            {
                _message.Dispose();
            }
        }

        public void Discard(Dictionary<string, object> annotations)
        {
            try
            {
                if (_link.IsClosed)
                {
                    throw new ConsumerException("Link is closed");
                }

                Utils.ValidateMessageAnnotations(annotations);

                Fields messageAnnotations = new();
                foreach (KeyValuePair<string, object> kvp in annotations)
                {
                    messageAnnotations.Add(new Symbol(kvp.Key), kvp.Value);
                }

                _link.Modify(_message, true, true, messageAnnotations);
                _unsettledMessageCounter.Decrement();
            }
            finally
            {
                _message.Dispose();
            }
        }

        public void Requeue()
        {
            try
            {
                if (_link.IsClosed)
                {
                    throw new ConsumerException("Link is closed");
                }

                _link.Release(_message);
                _unsettledMessageCounter.Decrement();
            }
            finally
            {
                _message.Dispose();
            }
        }

        public void Requeue(Dictionary<string, object> annotations)
        {
            try
            {
                if (_link.IsClosed)
                {
                    throw new ConsumerException("Link is closed");
                }

                Utils.ValidateMessageAnnotations(annotations);

                Fields messageAnnotations = new();
                foreach (var kvp in annotations)
                {
                    messageAnnotations.Add(new Symbol(kvp.Key), kvp.Value);
                }

                _link.Modify(_message, false, false, messageAnnotations);
                _unsettledMessageCounter.Decrement();
            }
            finally
            {
                _message.Dispose();
            }
        }
    }
}
