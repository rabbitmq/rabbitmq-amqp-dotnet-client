// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests.Consumer;

/// <summary>
/// Unit tests for <see cref="TimeoutDeliveryContext"/>.
/// These tests do not require a running broker.
/// </summary>
public class TimeoutDeliveryContextTests
{
    /// <summary>
    /// Discard/Requeue/Batch all throw before touching the link or message,
    /// so null is safe for the constructor arguments in those tests.
    /// </summary>
    private static TimeoutDeliveryContext CreateContext() =>
        new(null!, null!, new UnsettledMessageCounter(), metricsReporter: null);

    [Fact]
    public void Discard_throws_InvalidOperationException()
    {
        TimeoutDeliveryContext ctx = CreateContext();
        Assert.Throws<InvalidOperationException>(() => ctx.Discard());
    }

    [Fact]
    public void Discard_with_annotations_throws_InvalidOperationException()
    {
        TimeoutDeliveryContext ctx = CreateContext();
        Assert.Throws<InvalidOperationException>(
            () => ctx.Discard(new Dictionary<string, object> { ["x-reason"] = "bad" }));
    }

    [Fact]
    public void Requeue_throws_InvalidOperationException()
    {
        TimeoutDeliveryContext ctx = CreateContext();
        Assert.Throws<InvalidOperationException>(() => ctx.Requeue());
    }

    [Fact]
    public void Requeue_with_annotations_throws_InvalidOperationException()
    {
        TimeoutDeliveryContext ctx = CreateContext();
        Assert.Throws<InvalidOperationException>(
            () => ctx.Requeue(new Dictionary<string, object> { ["x-reason"] = "retry" }));
    }

    [Fact]
    public void Batch_throws_InvalidOperationException()
    {
        TimeoutDeliveryContext ctx = CreateContext();
        Assert.Throws<InvalidOperationException>(() => ctx.Batch());
    }
}
