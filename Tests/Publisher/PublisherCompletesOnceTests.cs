// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// Deterministic tests for https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues/191
// No broker needed: the native sender link is replaced by AmqpPublisher.SendOverride.

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests.Publisher;

public class PublisherCompletesOnceTests
{
    private sealed class RecordingMetricsReporter : IMetricsReporter
    {
        public ConcurrentQueue<IMetricsReporter.PublishDispositionValue> Dispositions { get; } = new();
        public int PublishedCount;

        public void ConnectionOpened() { }
        public void ConnectionClosed() { }
        public void PublisherOpened() { }
        public void PublisherClosed() { }
        public void ConsumerOpened() { }
        public void ConsumerClosed() { }
        public void Published(TimeSpan elapsed) => System.Threading.Interlocked.Increment(ref PublishedCount);
        public void PublishDisposition(IMetricsReporter.PublishDispositionValue disposition) =>
            Dispositions.Enqueue(disposition);
        public void Consumed(TimeSpan elapsed) { }
        public void ConsumeDisposition(IMetricsReporter.ConsumeDispositionValue disposition) { }
    }

    private static AmqpPublisher CreateOpenPublisher(Action<Message, OutcomeCallback, object> send,
        IMetricsReporter? metrics = null)
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create().Build();
        var connection = new AmqpConnection(settings, null);
        return new AmqpPublisher(connection, "/queues/fake", metrics) { SendOverride = send, State = State.Open };
    }

    private static Amqp.Framing.Error IllegalState() =>
        new(new Amqp.Types.Symbol("amqp:illegal-state"));

    // Callback runs first (e.g. Delivery.ReleaseAll on detach), then Send throws.
    [Fact]
    public async Task CallbackThenSendThrowsKeepsFirstOutcome()
    {
        var metrics = new RecordingMetricsReporter();
        AmqpPublisher publisher = CreateOpenPublisher((msg, cb, state) =>
        {
            cb(null!, msg, new Released(), state);
            throw new InvalidOperationException("session ended");
        }, metrics);

        PublishResult result = await publisher.PublishAsync(new AmqpMessage("x"));

        Assert.Equal(OutcomeState.Released, result.Outcome.State);
        Assert.Equal(new[] { IMetricsReporter.PublishDispositionValue.RELEASED }, metrics.Dispositions.ToArray());
        Assert.Equal(1, metrics.PublishedCount);
    }

    [Fact]
    public async Task CallbackThenSendThrowsAmqpExceptionKeepsFirstOutcome()
    {
        var metrics = new RecordingMetricsReporter();
        AmqpPublisher publisher = CreateOpenPublisher((msg, cb, state) =>
        {
            cb(null!, msg, new Accepted(), state);
            throw new AmqpException(IllegalState());
        }, metrics);

        PublishResult result = await publisher.PublishAsync(new AmqpMessage("x"));

        Assert.Equal(OutcomeState.Accepted, result.Outcome.State);
        // the catch must not add a second (REJECTED) disposition
        Assert.Equal(new[] { IMetricsReporter.PublishDispositionValue.ACCEPTED }, metrics.Dispositions.ToArray());
        Assert.Equal(1, metrics.PublishedCount);
    }

    // Send throws after the delivery was written, then the late callback fires on the reader
    // thread. It must not throw: an exception there closes the whole connection.
    [Fact]
    public async Task SendThrowsThenLateCallbackDoesNotThrow()
    {
        var metrics = new RecordingMetricsReporter();
        OutcomeCallback? captured = null;
        object? capturedState = null;
        AmqpPublisher publisher = CreateOpenPublisher((msg, cb, state) =>
        {
            captured = cb;
            capturedState = state;
            throw new AmqpException(IllegalState());
        }, metrics);

        PublishResult result = await publisher.PublishAsync(new AmqpMessage("x"));
        Assert.Equal(OutcomeState.Rejected, result.Outcome.State);

        Assert.NotNull(captured);
        Exception? thrown = Record.Exception(() =>
            captured!(null!, new Message(), new Released(), capturedState!));
        Assert.Null(thrown);
        // the late callback must not add a second (RELEASED) disposition or a Published timing
        Assert.Equal(new[] { IMetricsReporter.PublishDispositionValue.REJECTED }, metrics.Dispositions.ToArray());
        Assert.Equal(0, metrics.PublishedCount);
    }

    // Same as above, but Send throws a generic (non-Amqp) exception, which completes the task
    // via TrySetException. The late callback must still not throw.
    [Fact]
    public async Task SendThrowsGenericExceptionThenLateCallbackDoesNotThrow()
    {
        var metrics = new RecordingMetricsReporter();
        OutcomeCallback? captured = null;
        object? capturedState = null;
        AmqpPublisher publisher = CreateOpenPublisher((msg, cb, state) =>
        {
            captured = cb;
            capturedState = state;
            throw new InvalidOperationException("session ended");
        }, metrics);

        await Assert.ThrowsAsync<PublisherException>(() => publisher.PublishAsync(new AmqpMessage("x")));

        Assert.NotNull(captured);
        Exception? thrown = Record.Exception(() =>
            captured!(null!, new Message(), new Released(), capturedState!));
        Assert.Null(thrown);
        // the task faulted, so the late callback must not report any metric
        Assert.Empty(metrics.Dispositions);
        Assert.Equal(0, metrics.PublishedCount);
    }
}
