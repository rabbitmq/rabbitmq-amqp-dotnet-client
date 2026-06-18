// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// Quorum Queue Delayed Retry via IContext (RabbitMQ 4.3+)
//
// This example demonstrates the IContext disposition methods for delayed retries:
//
//   context.DelayedRetry()
//     Sends AMQP 1.0 modified{delivery-failed=true, undeliverable-here=false}.
//     Increments the broker's delivery-count for this message, which is used for
//     dead-letter routing (delivery limit) and, when the queue is configured with
//     x-delayed-retry-type=failed, for linear back-off redelivery delays.
//
//   context.DelayedRetry(TimeSpan delay)
//     Same AMQP disposition, but also sets the x-opt-delivery-time message
//     annotation (absolute Unix timestamp in milliseconds = DateTimeOffset.UtcNow + delay)
//     so the broker waits exactly `delay` before redelivering this specific message.
//
// Note: Full delay support via x-delayed-retry-type=failed requires a future client
// release that enables QuorumQueueDelayedRetryType.Failed. This example shows the
// redelivery behavior on a plain quorum queue (messages are re-enqueued immediately).
//
// Queue arguments used:
//   x-quorum-delivery-limit : 4  — dead-letter after 4 failed deliveries
//
// Run: dotnet run

using System.Diagnostics;
using System.Globalization;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

// ── tracing ──────────────────────────────────────────────────────────────────
Trace.TraceLevel = TraceLevel.Warning;
ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine(string.Format(CultureInfo.InvariantCulture, f, a ?? []));

// ── connect ───────────────────────────────────────────────────────────────────
IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId("qq-delayed-retry-context-example").Build());

IConnection connection = await environment.CreateConnectionAsync();
Console.WriteLine($"[{Now()}] Connected to the broker");

// ── declare queue ─────────────────────────────────────────────────────────────
IManagement management = connection.Management();
const string queueName = "qq-delayed-retry-context-example";
const int minTime = 5;
const int maxTime = 10;

// A delivery limit of 4 means the message is dead-lettered after 4 failed
// deliveries (i.e., after 4 calls to DelayedRetry()).
IQueueSpecification queueSpec = management.Queue(queueName)
    .Type(QueueType.QUORUM)
    .Quorum()
    .DelayedRetryType(QuorumQueueDelayedRetryType.Failed)
    .DelayedRetryMin(TimeSpan.FromSeconds(minTime))
    .DelayedRetryMax(TimeSpan.FromSeconds(maxTime))
    .DeliveryLimit(5)
    .Queue();

await queueSpec.DeclareAsync();
Console.WriteLine($"[{Now()}] Queue '{queueName}' declared (delivery-limit=4)");
Console.WriteLine();

// ── consumer ──────────────────────────────────────────────────────────────────
// Message processing strategy per acquired-count:
//   0  → context.DelayedRetry()                    – signal delivery failure
//   1  → context.DelayedRetry(TimeSpan.FromSeconds(2)) – explicit override delay
//   2+ → context.Accept()                           – done
IConsumer consumer = await connection.ConsumerBuilder()
    .Queue(queueName)
    .MessageHandler((context, message) =>
    {
        long deliveryCount = 0;
        try
        {
            deliveryCount = (long)message.DeliveryCount();
        }
        catch
        {
            /* not present on first delivery */
        }

        string msgId = message.BodyAsString();

        switch (deliveryCount)
        {
            case 0:
                // Override the delivery time for this specific message.
                // The broker will wait at least 7 seconds before redelivering.

                Console.WriteLine(
                    $"[{Now()}] {msgId} delivery-count={deliveryCount} → per message DelayedRetry: 7s ");
                context.DelayedRetry(TimeSpan.FromSeconds(7));
                break;

            case 1:
            case 2:
                // Signal to the broker that delivery failed.
                // With x-delayed-retry-type=failed this also applies the queue's
                // linear back-off delay before the next redelivery.
                // server side calculation:
                // delay =  min(delayed-retry-min * delivery-count, delayed-retry-max)
                int delay = Math.Min(minTime * (int)deliveryCount, maxTime);
                Console.WriteLine(
                    $"[{Now()}] {msgId} delivery-count={deliveryCount} → queue configuration DelayedRetry: {delay}s)");
                context.DelayedRetry();
                break;

            default:
                Console.WriteLine(
                    $"[{Now()}] {msgId} delivery-count={deliveryCount} → Accept ✓");
                context.Accept();
                break;
        }

        return Task.CompletedTask;
    })
    .BuildAndStartAsync();

// ── publisher ─────────────────────────────────────────────────────────────────
IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

const int totalMessages = 2;
Console.WriteLine($"[{Now()}] Publishing {totalMessages} messages …");
Console.WriteLine();

for (int i = 0; i < totalMessages; i++)
{
    var message = new AmqpMessage($"msg#{i}");
    PublishResult pr = await publisher.PublishAsync(message);
    Console.WriteLine(pr.Outcome.State == OutcomeState.Accepted
        ? $"[{Now()}] [Publisher] msg#{i} confirmed"
        : $"[{Now()}] [Publisher] msg#{i} outcome: {pr.Outcome.State}");
}

Console.WriteLine();
Console.WriteLine($"[{Now()}] Waiting for retries to complete …");
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
