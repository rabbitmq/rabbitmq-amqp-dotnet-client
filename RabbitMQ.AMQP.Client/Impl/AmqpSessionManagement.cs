// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class AmqpSessionManagement
    {
        private readonly AmqpConnection _amqpConnection;
        private readonly int _maxSessionsPerItem;
        private readonly ConcurrentBag<Session> _sessions = new();

        internal AmqpSessionManagement(AmqpConnection amqpConnection, int maxSessionsPerItem)
        {
            _amqpConnection = amqpConnection;
            _maxSessionsPerItem = maxSessionsPerItem;
        }

        // TODO cancellation token
        internal async Task<Session> GetOrCreateSessionAsync()
        {
            Session rv;

            if (_sessions.Count >= _maxSessionsPerItem)
            {
                rv = _sessions.First();
            }
            else
            {
                TaskCompletionSource<ISession> sessionBeginTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
                void OnBegin(ISession session, Begin peerBegin)
                {
                    sessionBeginTcs.SetResult(session);
                }

                rv = new Session(_amqpConnection.NativeConnection, GetDefaultBegin(), OnBegin);
                // TODO cancellation token
                ISession awaitedSession = await sessionBeginTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                _sessions.Add(rv);
            }

            return rv;
        }

        internal void ClearSessions()
        {
            // TODO close open sessions?
            _sessions.Clear();
        }

        // Note: these values come from Amqp.NET
        static Begin GetDefaultBegin()
        {
            return new Begin()
            {
                IncomingWindow = 2048,
                OutgoingWindow = 2048,
                HandleMax = 1024,
                NextOutgoingId = uint.MaxValue - 2u
            };
        }
    }
}
