// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// Deterministic tests for https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues/191
// No broker needed: the native sender link is replaced by AmqpPublisher.SendOverride.

using System;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests.Publisher;

public class PublisherCompletesOnceTests
{
    private static AmqpPublisher CreateOpenPublisher(Action<Message, OutcomeCallback, object> send)
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create().Build();
        var connection = new AmqpConnection(settings, null);
        return new AmqpPublisher(connection, "/queues/fake", null) { SendOverride = send, State = State.Open };
    }

    private static Amqp.Framing.Error IllegalState() =>
        new(new Amqp.Types.Symbol("amqp:illegal-state"));

    // Callback runs first (e.g. Delivery.ReleaseAll on detach), then Send throws.
    [Fact]
    public async Task CallbackThenSendThrowsKeepsFirstOutcome()
    {
        AmqpPublisher publisher = CreateOpenPublisher((msg, cb, state) =>
        {
            cb(null!, msg, new Released(), state);
            throw new InvalidOperationException("session ended");
        });

        PublishResult result = await publisher.PublishAsync(new AmqpMessage("x"));

        Assert.Equal(OutcomeState.Released, result.Outcome.State);
    }

    [Fact]
    public async Task CallbackThenSendThrowsAmqpExceptionKeepsFirstOutcome()
    {
        AmqpPublisher publisher = CreateOpenPublisher((msg, cb, state) =>
        {
            cb(null!, msg, new Accepted(), state);
            throw new AmqpException(IllegalState());
        });

        PublishResult result = await publisher.PublishAsync(new AmqpMessage("x"));

        Assert.Equal(OutcomeState.Accepted, result.Outcome.State);
    }

    // Send throws after the delivery was written, then the late callback fires on the reader
    // thread. It must not throw: an exception there closes the whole connection.
    [Fact]
    public async Task SendThrowsThenLateCallbackDoesNotThrow()
    {
        OutcomeCallback? captured = null;
        object? capturedState = null;
        AmqpPublisher publisher = CreateOpenPublisher((msg, cb, state) =>
        {
            captured = cb;
            capturedState = state;
            throw new AmqpException(IllegalState());
        });

        PublishResult result = await publisher.PublishAsync(new AmqpMessage("x"));
        Assert.Equal(OutcomeState.Rejected, result.Outcome.State);

        Assert.NotNull(captured);
        Exception? thrown = Record.Exception(() =>
            captured!(null!, new Message(), new Released(), capturedState!));
        Assert.Null(thrown);
    }
}
