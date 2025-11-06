// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.DirectReply
{
    public class DirectReplyTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        [SkippableFact]
        public async Task ValidateDirectReplyQName()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            var amqpConnection = (_connection as AmqpConnection);
            Skip.IfNot(amqpConnection is { _featureFlags.IsDirectReplyToSupported: true },
                "DirectReply is not supported by the connection.");

            IConsumer consumer = await _connection.ConsumerBuilder()
                .DirectReplyTo(true)
                .MessageHandler((IContext _, IMessage _) => Task.CompletedTask)
                .BuildAndStartAsync();

            Assert.Contains("amq.rabbitmq.reply-to", consumer.Queue);
        }

        [Fact]
        public async Task UseDirectReplyToReceiveAMessage()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            TaskCompletionSource<IMessage> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            var amqpConnection = (_connection as AmqpConnection);
            Skip.IfNot(amqpConnection is { _featureFlags.IsDirectReplyToSupported: true },
                "DirectReply is not supported by the connection.");

            IConsumer consumer = await _connection.ConsumerBuilder()
                .DirectReplyTo(true)
                .MessageHandler((IContext _, IMessage msg) =>
                {
                    tcs.SetResult(msg);
                    return Task.CompletedTask;
                }).BuildAndStartAsync();

            Assert.Contains("amq.rabbitmq.reply-to", consumer.Queue);
        }
    }
}
