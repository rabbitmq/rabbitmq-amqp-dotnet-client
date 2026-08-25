// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// Delayed Queue example (x-queue-type: delayed, requires Tanzu RabbitMQ 4.x or later)
//
// Declares a delayed queue via IQueueSpecification.Delayed(), publishes a few
// messages, and consumes them back. Declaring against a broker that does not
// support the "delayed" queue type returns a 409 Precondition Failed.
//
// Run: dotnet run
// Full path example: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/tree/main/docs/Examples/DelayedQueue/

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
    ConnectionSettingsBuilder.Create().ContainerId("delayed-queue-example").Build());

IConnection connection = await environment.CreateConnectionAsync();
Console.WriteLine($"[{Now()}] Connected to the broker");

// ── declare queue ─────────────────────────────────────────────────────────────
IManagement management = connection.Management();
const string queueName = "delayed-queue-example";

IQueueSpecification queueSpec = management.Queue(queueName)
    .Delayed()
        .DeliveryLimit(3)
        .QuorumInitialGroupSize(3)
    .Queue();

await queueSpec.DeclareAsync();
Console.WriteLine($"[{Now()}] Queue '{queueName}' declared (x-queue-type=delayed)");
Console.WriteLine();

// ── consumer ──────────────────────────────────────────────────────────────────
IConsumer consumer = await connection.ConsumerBuilder()
    .Queue(queueName)
    .MessageHandler((context, message) =>
    {
        Console.WriteLine($"[{Now()}] [Consumer] received: {message.BodyAsString()}");
        context.Accept();
        return Task.CompletedTask;
    })
    .BuildAndStartAsync();

// ── publisher ─────────────────────────────────────────────────────────────────
IPublisher publisher = await connection.PublisherBuilder().Queue(queueSpec).BuildAsync();

const int totalMessages = 5;
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
