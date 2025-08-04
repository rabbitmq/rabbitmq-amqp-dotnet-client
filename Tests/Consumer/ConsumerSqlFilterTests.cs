// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer
{
    public class ConsumerSqlFilterTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        // This class is a placeholder for SQL filter tests.
        // The actual implementation of SQL filter tests will depend on the specific requirements and context.
        // For example, it could involve testing SQL queries against a mock database or validating SQL syntax.

        // Example test method (to be implemented):
        [SkippableFact]
        [Trait("Category", "SqlFilter")]
        public async Task TestSqlFilterFunctionality()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            // cast to AMQPConnection to use Skip.If
            var amqpConnection = (_connection as AmqpConnection);
            Skip.IfNot(amqpConnection is { _featureFlags.IsSqlFeatureEnabled: true },
                "SQL filter is not supported by the connection.");

            IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
            await q.DeclareAsync();
            TaskCompletionSource<IMessage> tcs =
                new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(_queueName)
                .Stream().Filter().Sql("properties.user_id = 'John'").Stream().Offset(StreamOffsetSpecification.First)
                .Builder().MessageHandler((IContext ctx, IMessage msg) =>
                {
                    tcs.SetResult(msg);
                    // Here you would implement the logic to handle messages that match the SQL filter.
                    // For example, you could validate that the message content matches expected SQL criteria.
                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            IPublisher publisher = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();

            var msgNotInTheFilter = new AmqpMessage("Test message for SQL filter")
                .Property("user_id", "Gas"); // This property should not match the SQL filter
            await publisher.PublishAsync(msgNotInTheFilter);
            var msgInTheFilter = new AmqpMessage("Test message for NOT SQL filter")
                .Property("user_id", "John"); // This property should match the SQL filter
            await publisher.PublishAsync(msgInTheFilter);
            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

            Assert.Equal("Test message for SQL filter", tcs.Task.Result.BodyAsString());
            Assert.Equal("John", tcs.Task.Result.Property("user_id"));
            await consumer.CloseAsync().ConfigureAwait(false);
            await publisher.CloseAsync().ConfigureAwait(false);
            await q.DeleteAsync().ConfigureAwait(false);
            await _connection.CloseAsync().ConfigureAwait(false);
        }
    }
}
