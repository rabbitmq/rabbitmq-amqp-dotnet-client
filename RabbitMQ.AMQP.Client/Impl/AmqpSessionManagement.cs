// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
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

        // lock during session creation
        private readonly SemaphoreSlim _semaphoreSlim = new(1);

        internal AmqpSessionManagement(AmqpConnection amqpConnection, int maxSessionsPerItem)
        {
            _amqpConnection = amqpConnection;
            _maxSessionsPerItem = maxSessionsPerItem;
        }

        // TODO cancellation token
        internal async Task<Session> GetOrCreateSessionAsync()
        {
            await _semaphoreSlim.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            try
            {
                Session rv;

                if (_sessions.Count >= _maxSessionsPerItem)
                {
                    rv = _sessions.First();
                }
                else
                {
                    TaskCompletionSource<ISession> sessionBeginTcs = Utils.CreateTaskCompletionSource<ISession>();

                    void OnBegin(ISession session, Begin peerBegin)
                    {
                        sessionBeginTcs.SetResult(session);
                    }

                    rv = new Session(_amqpConnection.NativeConnection, GetDefaultBegin(), OnBegin);
                    // TODO cancellation token
                    ISession awaitedSession =
                        await sessionBeginTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                    _sessions.Add(rv);
                }

                return rv;
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        internal void ClearSessions()
        {
            _semaphoreSlim.Wait();
            try
            {
                // TODO close open sessions?
                _sessions.Clear();
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        // sessions count 
        internal int GetSessionsCount()
        {
            _semaphoreSlim.Wait();
            try
            {
                return _sessions.Count;
            }
            finally
            {
                _semaphoreSlim.Release();
            }
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
