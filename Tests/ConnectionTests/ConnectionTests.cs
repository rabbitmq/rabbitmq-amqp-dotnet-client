// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.ConnectionTests;

public class ConnectionTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task ThrowAmqpClosedExceptionWhenItemIsClosed()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName);
        await queueSpec.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();
        await publisher.CloseAsync();
        publisher.Dispose();

        await Assert.ThrowsAsync<AmqpNotOpenException>(() =>
        {
            var message = new AmqpMessage("Hello wold!");
            return publisher.PublishAsync(message);
        });

        await queueSpec.DeleteAsync();
        await _connection.CloseAsync();
        Assert.Empty(_connection.Publishers);

        await Assert.ThrowsAsync<AmqpNotOpenException>(() =>
            _connection.PublisherBuilder().Queue("ThrowAmqpClosedExceptionWhenItemIsClosed").BuildAsync());

        await Assert.ThrowsAsync<AmqpNotOpenException>(async () =>
            await _management.Queue().Name("ThrowAmqpClosedExceptionWhenItemIsClosed").DeclareAsync());
    }
}
