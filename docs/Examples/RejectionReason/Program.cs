// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// Rejection Reason example (requires RabbitMQ 4.3 or later)
//
// When a quorum queue rejects a published message (e.g. because max-length is reached
// and overflow-strategy is reject-publish), RabbitMQ 4.3+ includes the queue name and
// the specific rejection reason inside the AMQP Rejected outcome.
//
// This example demonstrates:
//   1. Declaring a quorum queue with a max-length of 5 and reject-publish overflow.
//   2. Filling the queue so the next publish is rejected.
//   3. Reading PublishOutcome.Exception (AmqpMessageRejectedException) to obtain
//      the human-readable rejection reason and the RejectedBy queue name.
//
// See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#amqp-rejection-reason

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Starting Rejection Reason example (requires RabbitMQ 4.3+)...");

const string containerId = "rejection-reason-example";
const string queueName = "rejection-reason-demo-queue";
const int maxLength = 5;

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync();
Trace.WriteLine(TraceLevel.Information, $"Connected: {connection}");

IManagement management = connection.Management();

// Declare a quorum queue with a max-length of 5 messages.
// When the queue is full, new publishes are rejected (reject-publish strategy).
IQueueSpecification queueSpec = management
    .Queue(queueName)
    .Type(QueueType.QUORUM)
    .MaxLength(maxLength)
    .OverflowStrategy(OverFlowStrategy.RejectPublish);

await queueSpec.DeclareAsync();
Trace.WriteLine(TraceLevel.Information,
    $"Queue '{queueName}' declared (quorum, max-length={maxLength}, overflow=reject-publish).");

IPublisher publisher = await connection.PublisherBuilder().Queue(queueSpec).BuildAsync();

// ------------------------------------------------------------------------------------
// Fill the queue up to max-length. All of these should be accepted.
// ------------------------------------------------------------------------------------
Trace.WriteLine(TraceLevel.Information, $"Publishing {maxLength} messages to fill the queue...");

for (int i = 1; i <= maxLength + 1; i++)
{
    var msg = new AmqpMessage($"message-{i}");
    PublishResult pr = await publisher.PublishAsync(msg);

    if (pr.Outcome.State == OutcomeState.Accepted)
    {
        Trace.WriteLine(TraceLevel.Information, $"  [{i}/{maxLength}] Accepted: {msg.BodyAsString()}");
    }
    else
    {
        Trace.WriteLine(TraceLevel.Warning,
            $"  [{i}/{maxLength}] Unexpected outcome '{pr.Outcome.State}' for: {msg.BodyAsString()}");
    }
}

// ------------------------------------------------------------------------------------
// Now the queue is full. The next publish should be rejected.
// On RabbitMQ 4.3+ the Rejected outcome carries the queue name and rejection reason.
// ------------------------------------------------------------------------------------
Trace.WriteLine(TraceLevel.Information, "Queue is full. Publishing one more message — expecting rejection...");

var overflow = new AmqpMessage("overflow-message");
PublishResult rejectedResult = await publisher.PublishAsync(overflow);

switch (rejectedResult.Outcome.State)
{
    case OutcomeState.Rejected:
        Trace.WriteLine(TraceLevel.Warning, "Message was rejected (as expected).");

        // Error carries the raw AMQP error code and description (available on all broker versions).
        if (rejectedResult.Outcome.Error is { } error)
        {
            Trace.WriteLine(TraceLevel.Warning,
                $"  AMQP error code : {error.ErrorCode}");
            Trace.WriteLine(TraceLevel.Warning,
                $"  AMQP description: {error.Description ?? "(none)"}");
        }

        // Exception is populated on RabbitMQ 4.3+ and contains structured rejection details.
        if (rejectedResult.Outcome.Exception is { } ex)
        {
            Trace.WriteLine(TraceLevel.Warning,
                $"  Rejection reason : {ex.Reason}, message: {ex.Message}");
            Trace.WriteLine(TraceLevel.Warning,
                $"  Rejected by queue: {ex.RejectedBy ?? "(not provided)"}");
        }
        else
        {
            Trace.WriteLine(TraceLevel.Information,
                "  No structured rejection details (broker may be older than 4.3).");
        }

        break;

    case OutcomeState.Accepted:
        Trace.WriteLine(TraceLevel.Information, "Message was unexpectedly accepted.");
        break;

    case OutcomeState.Released:
        Trace.WriteLine(TraceLevel.Information, "Message was released (no queue matched).");
        break;

    default:
        throw new ArgumentOutOfRangeException();
}

// ------------------------------------------------------------------------------------
// Cleanup
// ------------------------------------------------------------------------------------
await publisher.CloseAsync();
publisher.Dispose();

await queueSpec.DeleteAsync();
Trace.WriteLine(TraceLevel.Information, $"Queue '{queueName}' deleted.");

await environment.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Example finished.");
