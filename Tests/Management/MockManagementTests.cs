// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Framing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Message = Amqp.Message;

namespace Tests;

public class MockManagementTests()
{
    public class TestAmqpManagement() : AmqpManagement(new AmqpManagementParameters(null!))
    {
        internal protected override async Task InternalSendAsync(Message message, TimeSpan timeout)
        {
            await Task.Delay(1000);
        }
    }

    public class TestAmqpManagementOpen : AmqpManagement
    {
        public TestAmqpManagementOpen() : base(new AmqpManagementParameters(null!))
        {
            State = State.Open;
        }

        internal protected override async Task InternalSendAsync(Message message, TimeSpan timeout)
        {
            await Task.Delay(1000);
        }

        public void TestHandleResponseMessage(Message msg)
        {
            HandleResponseMessage(msg);
        }
    }

    [Fact]
    public async Task RaiseOperationCanceledException()
    {
        var management = new TestAmqpManagementOpen();
        var message = new Message() { Properties = new Properties() { MessageId = "a_random_id", } };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await management.RequestAsync(message, [200], TimeSpan.FromSeconds(1)));
        await management.CloseAsync();
    }

    /// <summary>
    /// Test to raise a ModelException based on checking the response
    /// the message _must_ respect the following rules:
    /// - id and correlation id should match
    /// - subject _must_ be a number
    /// The test validate the following cases:
    ///  - subject is not a number
    ///  - code is not in the expected list
    ///  - correlation id is not the same as the message id
    /// </summary>
    [Fact]
    public void RaiseModelException()
    {
        var management = new TestAmqpManagement();
        const string messageId = "my_id";
        var sent = new Message() { Properties = new Properties() { MessageId = messageId, } };

        var receive = new Message()
        {
            Properties = new Properties() { CorrelationId = messageId, Subject = "Not_a_Number", }
        };

        Assert.Throws<ModelException>(() =>
            management.CheckResponse(sent, [], receive));

        receive.Properties.Subject = "200";
        Assert.Throws<InvalidCodeException>(() =>
            management.CheckResponse(sent, [201], receive));

        receive.Properties.CorrelationId = "not_my_id";
        Assert.Throws<ModelException>(() =>
            management.CheckResponse(sent, [], receive));
    }

    [Fact]
    public async Task RaiseInvalidCodeException()
    {
        var management = new TestAmqpManagementOpen();

        const string messageId = "my_id";
        var t = Task.Run(async () =>
        {
            await Task.Delay(1000);
            management.TestHandleResponseMessage(new Message()
            {
                Properties = new Properties()
                {
                    CorrelationId = messageId,
                    Subject = "506", // 506 is not a valid code
                }
            });
        });

        await Assert.ThrowsAsync<InvalidCodeException>(async () =>
            await management.RequestAsync(messageId, "", "", "",
                [200]));

        await t.WaitAsync(TimeSpan.FromMilliseconds(1000));
        await management.CloseAsync();
    }

    [Fact]
    public async Task RaiseManagementClosedException()
    {
        var management = new TestAmqpManagement();
        await Assert.ThrowsAsync<AmqpNotOpenException>(async () =>
           await management.RequestAsync(new Message(), [200]));
        Assert.Equal(State.Closed, management.State);
    }

    // ---- ProcessResponsesAsync fault-propagation tests ----

    public class TestAmqpManagementFaultingSession : AmqpManagement
    {
        public TestAmqpManagementFaultingSession() : base(new AmqpManagementParameters(null!))
        {
            State = State.Open;
        }

        protected override Task ProcessResponsesAsync()
            => Task.FromException(new InvalidOperationException("simulated receiver link fault"));

        public void StartProcessing() => BeginProcessingResponses();

        internal protected override async Task InternalSendAsync(Message message, TimeSpan timeout)
            => await Task.Delay(1);
    }

    public class TestAmqpManagementCleanSession : AmqpManagement
    {
        public TestAmqpManagementCleanSession() : base(new AmqpManagementParameters(null!))
        {
            State = State.Open;
        }

        protected override Task ProcessResponsesAsync() => Task.CompletedTask;

        public void StartProcessing() => BeginProcessingResponses();

        internal protected override async Task InternalSendAsync(Message message, TimeSpan timeout)
            => await Task.Delay(1);
    }

    [Fact]
    public async Task ProcessingSessionFault_TransitionsManagementToClosed()
    {
        var management = new TestAmqpManagementFaultingSession();
        var stateChangeTcs = Utils.CreateTaskCompletionSource<State>();
        management.ChangeState += (_, _, newState, _) => stateChangeTcs.TrySetResult(newState);

        management.StartProcessing();

        State arrived = await stateChangeTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(State.Closed, arrived);
    }

    [Fact]
    public async Task ProcessingSessionFault_FiresChangeStateWithError()
    {
        var management = new TestAmqpManagementFaultingSession();
        var errorTcs = Utils.CreateTaskCompletionSource<RabbitMQ.AMQP.Client.Error?>();
        management.ChangeState += (_, _, _, error) => errorTcs.TrySetResult(error);

        management.StartProcessing();

        RabbitMQ.AMQP.Client.Error? error = await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(error);
    }

    [Fact]
    public async Task ProcessingSessionCleanExit_DoesNotChangeState()
    {
        var management = new TestAmqpManagementCleanSession();
        bool stateChanged = false;
        management.ChangeState += (_, _, _, _) => stateChanged = true;

        management.StartProcessing();

        await Task.Delay(200);
        Assert.False(stateChanged);
        Assert.Equal(State.Open, management.State);
    }

    [Theory]
    [InlineData(QuorumQueueDelayedRetryType.Disabled, "disabled")]
    // [InlineData(QuorumQueueDelayedRetryType.All, "all")]
    // [InlineData(QuorumQueueDelayedRetryType.Failed, "failed")]
    [InlineData(QuorumQueueDelayedRetryType.Returned, "returned")]
    public void QuorumQueueDelayedRetryTypeSetsCorrectArgument(
        QuorumQueueDelayedRetryType retryType, string expectedValue)
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum().DelayedRetryType(retryType);

        Assert.Equal(expectedValue, spec.QueueArguments["x-delayed-retry-type"]);
    }

    [Fact]
    public void QuorumQueueDelayedRetryMinSetsCorrectArgument()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum().DelayedRetryMin(TimeSpan.FromSeconds(1));

        Assert.Equal(1000L, spec.QueueArguments["x-delayed-retry-min"]);
    }

    [Fact]
    public void QuorumQueueDelayedRetryMaxSetsCorrectArgument()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum().DelayedRetryMax(TimeSpan.FromSeconds(30));

        Assert.Equal(30000L, spec.QueueArguments["x-delayed-retry-max"]);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void QuorumQueueDelayedRetryMinThrowsForNonPositiveValue(int seconds)
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        Assert.Throws<ArgumentException>(() =>
            spec.Quorum().DelayedRetryMin(TimeSpan.FromSeconds(seconds)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void QuorumQueueDelayedRetryMaxThrowsForNonPositiveValue(int seconds)
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        Assert.Throws<ArgumentException>(() =>
            spec.Quorum().DelayedRetryMax(TimeSpan.FromSeconds(seconds)));
    }

    // ---- DeclareAsync delayed-retry argument validation tests ----
    // Validation rule: if any of x-delayed-retry-min, x-delayed-retry-max, or
    // x-delayed-redelivery-max is set, x-delayed-retry-type must also be set.

    /// <summary>
    /// Setting x-delayed-retry-min without x-delayed-retry-type must throw.
    /// </summary>
    [Fact]
    public async Task DeclareAsync_ThrowsInvalidOperationException_WhenDelayedRetryMinSetWithoutType()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum().DelayedRetryMin(TimeSpan.FromSeconds(1)).Queue().Name("test-queue");

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await spec.DeclareAsync());

        Assert.Contains("x-delayed-retry-type", ex.Message);
    }

    /// <summary>
    /// Setting x-delayed-retry-max without x-delayed-retry-type must throw.
    /// </summary>
    [Fact]
    public async Task DeclareAsync_ThrowsInvalidOperationException_WhenDelayedRetryMaxSetWithoutType()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum().DelayedRetryMax(TimeSpan.FromSeconds(30)).Queue().Name("test-queue");

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await spec.DeclareAsync());

        Assert.Contains("x-delayed-retry-type", ex.Message);
    }

    /// <summary>
    /// Setting x-delayed-redelivery-max without x-delayed-retry-type must throw.
    /// </summary>
    [Fact]
    public async Task DeclareAsync_ThrowsInvalidOperationException_WhenDelayedRedeliveryMaxSetWithoutType()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        // x-delayed-redelivery-max has no dedicated builder method; set it directly.
        spec.Name("test-queue");
        spec.QueueArguments["x-delayed-redelivery-max"] = 5000L;

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await spec.DeclareAsync());

        Assert.Contains("x-delayed-retry-type", ex.Message);
    }

    /// <summary>
    /// Setting x-delayed-retry-min together with x-delayed-retry-type must pass
    /// validation. The test confirms this by observing AmqpNotOpenException (thrown
    /// by the closed management when RequestAsync is reached) rather than
    /// InvalidOperationException (which would indicate a validation failure).
    /// </summary>
    [Fact]
    public async Task DeclareAsync_PassesValidation_WhenDelayedRetryMinSetWithType()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum()
            .DelayedRetryType(QuorumQueueDelayedRetryType.Returned)
            .DelayedRetryMin(TimeSpan.FromSeconds(1))
            .Queue()
            .Name("test-queue");

        // Validation passes → reaches RequestAsync → management is closed → AmqpNotOpenException
        await Assert.ThrowsAsync<AmqpNotOpenException>(async () => await spec.DeclareAsync());
    }

    /// <summary>
    /// Setting x-delayed-retry-max together with x-delayed-retry-type must pass validation.
    /// </summary>
    [Fact]
    public async Task DeclareAsync_PassesValidation_WhenDelayedRetryMaxSetWithType()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum()
            .DelayedRetryType(QuorumQueueDelayedRetryType.Returned)
            .DelayedRetryMax(TimeSpan.FromSeconds(30))
            .Queue()
            .Name("test-queue");

        await Assert.ThrowsAsync<AmqpNotOpenException>(async () => await spec.DeclareAsync());
    }

    /// <summary>
    /// Setting x-delayed-redelivery-max together with x-delayed-retry-type must pass validation.
    /// </summary>
    [Fact]
    public async Task DeclareAsync_PassesValidation_WhenDelayedRedeliveryMaxSetWithType()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum()
            .DelayedRetryType(QuorumQueueDelayedRetryType.Returned)
            .Queue()
            .Name("test-queue");
        spec.QueueArguments["x-delayed-redelivery-max"] = 5000L;

        await Assert.ThrowsAsync<AmqpNotOpenException>(async () => await spec.DeclareAsync());
    }

    /// <summary>
    /// Setting all three delayed retry arguments with a type must pass validation.
    /// </summary>
    [Fact]
    public async Task DeclareAsync_PassesValidation_WhenAllDelayedRetryArgumentsSetWithType()
    {
        var management = new TestAmqpManagement();
        var spec = new AmqpQueueSpecification(management);
        spec.Quorum()
            .DelayedRetryType(QuorumQueueDelayedRetryType.Returned)
            .DelayedRetryMin(TimeSpan.FromSeconds(1))
            .DelayedRetryMax(TimeSpan.FromSeconds(30))
            .Queue()
            .Name("test-queue");
        spec.QueueArguments["x-delayed-redelivery-max"] = 5000L;

        await Assert.ThrowsAsync<AmqpNotOpenException>(async () => await spec.DeclareAsync());
    }
}
