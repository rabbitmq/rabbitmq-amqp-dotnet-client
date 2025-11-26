// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Sessions
{
    public class SessionsTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(37)]
        public async Task CreateSessionSequentiallyShouldHaveTheMaxSessionsPerItem(int maxSessions)
        {
            Assert.NotNull(_connection);
            AmqpConnection? amqpConnection = _connection as AmqpConnection;
            Assert.NotNull(amqpConnection);
            AmqpSessionManagement s = new(amqpConnection, maxSessions);
            for (int i = 0; i < 100; i++)
            {
                await s.GetOrCreateSessionAsync();
            }

            Assert.Equal(maxSessions, s.GetSessionsCount());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(37)]
        public async Task CreateSessionInMultiThreadingShouldHaveTheMaxSessionsPerItem(int maxSessions)
        {
            Assert.NotNull(_connection);
            AmqpConnection? amqpConnection = _connection as AmqpConnection;
            Assert.NotNull(amqpConnection);
            AmqpSessionManagement s = new(amqpConnection, maxSessions);
            Task[] tasks = new Task[100];
            for (int i = 0; i < 100; i++)
            {
                tasks[i] = (Task.Run(async () => { await s.GetOrCreateSessionAsync(); }));
            }

            await Task.WhenAll(tasks);

            Assert.Equal(maxSessions, s.GetSessionsCount());
        }
    }
}
