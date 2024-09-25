// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public class ConsumerException : Exception
    {
        public ConsumerException(string message) : base(message)
        {
        }
    }

    public delegate Task MessageHandler(IContext context, IMessage message);

    public interface IConsumer : ILifeCycle
    {
        void Pause();
        void Unpause();
        long UnsettledMessageCount { get; }
    }

    public interface IContext
    {
        Task AcceptAsync();
        Task DiscardAsync();
        Task DiscardAsync(Dictionary<string, object> annotations);

        Task RequeueAsync();
        Task RequeueAsync(Dictionary<string, object> annotations);
    }
}
