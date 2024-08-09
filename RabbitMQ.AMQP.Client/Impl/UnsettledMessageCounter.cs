// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client.Impl
{
    public class UnsettledMessageCounter
    {
        private long _unsettledMessageCount;

        public long Get()
        {
            return Interlocked.Read(ref _unsettledMessageCount);
        }

        public void Increment()
        {
            Interlocked.Increment(ref _unsettledMessageCount);
        }

        public void Decrement()
        {
            Interlocked.Decrement(ref _unsettledMessageCount);
        }

        public void Reset()
        {
            Interlocked.Exchange(ref _unsettledMessageCount, 0);
        }
    }
}
