// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class UnsettledMessageCounter
    {
        private long _unsettledMessageCount;

        internal long Get()
        {
            return Interlocked.Read(ref _unsettledMessageCount);
        }

        internal void Increment()
        {
            Interlocked.Increment(ref _unsettledMessageCount);
        }

        internal void Decrement()
        {
            Interlocked.Decrement(ref _unsettledMessageCount);
        }

        internal void Reset()
        {
            Interlocked.Exchange(ref _unsettledMessageCount, 0);
        }
    }
}
