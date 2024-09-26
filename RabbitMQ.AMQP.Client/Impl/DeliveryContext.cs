// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Threading.Tasks;
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

        public Task AcceptAsync()
        {
            if (_link.IsClosed)
            {
                throw new ConsumerException("Link is closed");
            }

            Task acceptTask = Task.Run(() =>
            {
                _link.Accept(_message);
                _unsettledMessageCounter.Decrement();
                _message.Dispose();
            });

            return acceptTask;
        }

        public Task DiscardAsync()
        {
            if (_link.IsClosed)
            {
                throw new ConsumerException("Link is closed");
            }

            Task rejectTask = Task.Run(() =>
            {
                _link.Reject(_message);
                _unsettledMessageCounter.Decrement();
                _message.Dispose();
            });

            return rejectTask;
        }

        public Task DiscardAsync(Dictionary<string, object> annotations)
        {
            if (_link.IsClosed)
            {
                throw new ConsumerException("Link is closed");
            }

            Utils.ValidateMessageAnnotations(annotations);

            Task rejectTask = Task.Run(() =>
            {
                Fields messageAnnotations = new();
                foreach (var kvp in annotations)
                {
                    messageAnnotations.Add(new Symbol(kvp.Key), kvp.Value);
                }

                _link.Modify(_message, true, true, messageAnnotations);
                _unsettledMessageCounter.Decrement();
                _message.Dispose();
            });

            return rejectTask;
        }

        public Task RequeueAsync()
        {
            if (_link.IsClosed)
            {
                throw new ConsumerException("Link is closed");
            }

            Task requeueTask = Task.Run(() =>
            {
                _link.Release(_message);
                _unsettledMessageCounter.Decrement();
                _message.Dispose();
            });

            return requeueTask;
        }

        public Task RequeueAsync(Dictionary<string, object> annotations)
        {
            if (_link.IsClosed)
            {
                throw new ConsumerException("Link is closed");
            }
            Utils.ValidateMessageAnnotations(annotations);
            Task requeueTask = Task.Run(() =>
            {
                Fields messageAnnotations = new();
                foreach (var kvp in annotations)
                {
                    messageAnnotations.Add(new Symbol(kvp.Key), kvp.Value);
                }

                _link.Modify(_message, false, false, messageAnnotations);
                _unsettledMessageCounter.Decrement();
                _message.Dispose();
            });

            return requeueTask;
        }
    }
}
