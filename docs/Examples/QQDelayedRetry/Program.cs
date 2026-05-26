// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// Quorum Queue Delayed Retry (RabbitMQ 4.3+)
//
// When a consumer discards a message (increments its delivery count), the broker
// will delay redelivery using linear back-off:
//
//   delay = min(min_delay * delivery_count, max_delay)
//
// This prevents a fast retry storm when processing fails transiently.
// A per-message explicit delivery time can also be set via the
// "x-opt-delivery-time" message annotation (Unix timestamp in milliseconds).
//
// Queue arguments used:
//   x-delayed-retry-type : "failed"   — delay only messages whose delivery-count was incremented
//   x-delayed-retry-min  : 2000       — 2 s minimum delay
//   x-delayed-retry-max  : 10000      — 10 s maximum delay (cap)
//   x-max-delivery-limit : 5          — dead-letter after 5 failed attempts
//
// Expected output (approximate):
//   [Consumer] msg#0 delivery-count=0 → failing (Discard)
//   [Consumer] msg#0 delivery-count=1 → failing (Discard)   (~2 s later)
//   [Consumer] msg#0 delivery-count=2 → failing (Discard)   (~4 s later)
//   [Consumer] msg#0 delivery-count=3 → accepted            (~6 s later)
//
// Run: dotnet run

using System.Diagnostics;
using System.Globalization;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

// ── tracing ──────────────────────────────────────────────────────────────────
Trace.TraceLevel = TraceLevel.Warning; // suppress low-level AMQP frames
ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine(string.Format(CultureInfo.InvariantCulture, f, a ?? []));

// ── connect ───────────────────────────────────────────────────────────────────
IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId("qq-delayed-retry-example").Build());

IConnection connection = await environment.CreateConnectionAsync();
Console.WriteLine($"[{Now()}] Connected to the broker");

// ── declare queue ─────────────────────────────────────────────────────────────
// Uses DelayedRetryType.Failed so that only messages whose delivery-count is
// incremented (i.e. discarded via context.Discard()) are subject to the delay.
// Messages requeued without failure (context.Requeue()) are not delayed.
IManagement management = connection.Management();
const string queueName = "qq-delayed-retry-example";

IQueueSpecification queueSpec = management.Queue(queueName)
    .Quorum()
        .DelayedRetryType(QuorumQueueDelayedRetryType.Returned)
        .DelayedRetryMin(TimeSpan.FromSeconds(2))   // 2 s base delay
        .DelayedRetryMax(TimeSpan.FromSeconds(10))  // cap at 10 s
        .DeliveryLimit(5)                           // dead-letter after 5 attempts
    .Queue();


await queueSpec.DeclareAsync();
Console.WriteLine($"[{Now()}] Queue '{queueName}' declared");
Console.WriteLine($"[{Now()}] Delayed retry: type=failed, min=2 s, max=10 s, delivery-limit=5");
Console.WriteLine();

// ── consumer ──────────────────────────────────────────────────────────────────
// Accept a message only on the 4th delivery (acquired-count >= 3).
// On earlier deliveries, call context.Requeue() which sends a Modified outcome
// with the acquired-count incremented, triggering the delayed retry.
const int acceptOnAcquiredCount = 3;

IConsumer consumer = await connection.ConsumerBuilder()
    .Queue(queueName)
    .MessageHandler((context, message) =>
    {
        Console.WriteLine("+++++++++++++++++++++++++++++++++++");
        // RabbitMQ 4.3+ sets "x-acquired-count" on redeliveries.
        long acquiredCount = 0;
        try
        {
            acquiredCount = (long)message.Annotation("x-acquired-count"); 
            
            
        }
        catch { /* not present on the first delivery */ }

        string msgId = message.BodyAsString();

        if (acquiredCount < acceptOnAcquiredCount)
        {
            Console.WriteLine(
                $"[{Now()}] [Consumer] {msgId} acquired count={acquiredCount} → failing (Requeue). " +
                $"Next retry in ~2s");
           
            context.Requeue(); // increments acquired-count → triggers delayed retry
        }
        else
        {
            Console.WriteLine(
                $"[{Now()}] [Consumer] {msgId} acquired-count={acquiredCount} → accepted ✓");
            context.Accept();
        }

        return Task.CompletedTask;
    })
    .BuildAndStartAsync();

// ── publisher ─────────────────────────────────────────────────────────────────
IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

const int totalMessages = 1;
Console.WriteLine($"[{Now()}] Publishing {totalMessages} messages...");
Console.WriteLine();

for (int i = 0; i < totalMessages; i++)
{
    var message = new AmqpMessage($"msg#{i}");
    PublishResult pr = await publisher.PublishAsync(message);
    Console.WriteLine(pr.Outcome.State == OutcomeState.Accepted
        ? $"[{Now()}] [Publisher] msg#{i} confirmed by broker"
        : $"[{Now()}] [Publisher] msg#{i} outcome: {pr.Outcome.State}");
}

Console.WriteLine();
Console.WriteLine($"[{Now()}] Publishing done. Waiting for retries to complete...");
Console.WriteLine("Press Enter to delete the queue and exit.");
Console.ReadLine();

// ── cleanup ───────────────────────────────────────────────────────────────────
await publisher.CloseAsync();
publisher.Dispose();

await consumer.CloseAsync();
consumer.Dispose();

await queueSpec.DeleteAsync();
Console.WriteLine($"[{Now()}] Queue '{queueName}' deleted");

await environment.CloseAsync();
Console.WriteLine($"[{Now()}] Done");

static string Now() => DateTime.Now.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
