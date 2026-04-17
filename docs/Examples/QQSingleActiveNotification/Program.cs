// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// Quorum queue + Single Active Consumer: FLOW link-state notifications (`rabbitmq:active`)
// via IConsumerBuilder.SingleActiveConsumerStateChanged (RabbitMQ 4.3+).
//
// Usage (run each role in its own process; only one producer or one consumer per process):
//   dotnet run -- <producer|consumer> <queueName>
//
// Parameters:
//   1) Type: producer or consumer
//   2) queueName: shared quorum queue (same name in producer and consumer processes)
//
// Example:
//   dotnet run -- consumer 
//   dotnet run -- producer
// Run more than one consumer to see the single active consumer state change notifications in the console output.

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

if (args.Length < 1)
{
    Console.Error.WriteLine("Usage: QQSingleActiveNotification <producer|consumer>");
    return 1;
}

string mode = args[0].Trim().ToLowerInvariant();
const string queueName = "demo-qq-sac-notification"; // default queue name

if (mode is not ("producer" or "consumer"))
{
    Console.Error.WriteLine("First argument (type) must be \"producer\" or \"consumer\".");
    return 1;
}

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

string containerId = $"qq-sac-notification-{mode}-{Environment.ProcessId}";

Trace.WriteLine(TraceLevel.Information, "Starting QQ Single Active Consumer notification example...");

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync();

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

try
{
    if (mode == "producer")
    {
        await RunProducerAsync(connection, queueName).ConfigureAwait(false);
    }
    else
    {
        await RunConsumerAsync(connection, queueName, containerId).ConfigureAwait(false);
    }
}
finally
{
    await environment.CloseAsync().ConfigureAwait(false);
    Trace.WriteLine(TraceLevel.Information, "Environment closed");
}

return 0;

static IQueueSpecification CreateQueueSpec(IManagement management, string name) =>
    management.Queue(name)
        .Type(QueueType.QUORUM)
        .SingleActiveConsumer(true);

static async Task RunProducerAsync(IConnection connection, string name)
{
    IManagement management = connection.Management();
    IQueueSpecification queueSpec = CreateQueueSpec(management, name);
    await queueSpec.DeclareAsync().ConfigureAwait(false);
    Trace.WriteLine(TraceLevel.Information,
        $"Queue {name} declared (quorum, single-active-consumer).");

    IPublisher publisher = await connection.PublisherBuilder().Queue(name).BuildAsync().ConfigureAwait(false);

    const int total = 3000;
    for (int i = 0; i < total; i++)
    {
        await Task.Delay(500).ConfigureAwait(false);
        var message = new AmqpMessage($"SAC demo message #{i}");
        PublishResult pr = await publisher.PublishAsync(message).ConfigureAwait(false);

        switch (pr.Outcome.State)
        {
            case OutcomeState.Accepted:
                Trace.WriteLine(TraceLevel.Information,
                    $"[Producer] Published and confirmed: {message.BodyAsString()}");
                break;
            case OutcomeState.Released:
                Trace.WriteLine(TraceLevel.Information,
                    $"[Producer] Released: {message.BodyAsString()}");
                break;
            case OutcomeState.Rejected:
                Trace.WriteLine(TraceLevel.Error,
                    $"[Producer] Rejected: {message.BodyAsString()} — {pr.Outcome.Error}");
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    await publisher.CloseAsync().ConfigureAwait(false);
    publisher.Dispose();
    Trace.WriteLine(TraceLevel.Information, "Producer finished publishing.");
}

static async Task RunConsumerAsync(IConnection connection, string name, string containerId)
{
    IManagement management = connection.Management();
    IQueueSpecification queueSpec = CreateQueueSpec(management, name);
    await queueSpec.DeclareAsync().ConfigureAwait(false);
    Trace.WriteLine(TraceLevel.Information,
        $"Queue {name} declared (quorum, single-active-consumer).");

    IConsumer consumer = await connection.ConsumerBuilder()
        .Queue(queueSpec).Quorum()
        .SingleActiveConsumerStateChanged((c, isActive) =>
        {
            string state = isActive ? "ACTIVE (this link delivers messages)" : "STANDBY";
            Trace.WriteLine(TraceLevel.Information, "********************************");
            Trace.WriteLine(TraceLevel.Information,
                $"Single Active Consumer state: {state} containerId={containerId}");
            Trace.WriteLine(TraceLevel.Information, "********************************");
        }).Builder()
        .MessageHandler((context, message) =>
        {
            Trace.WriteLine(TraceLevel.Information,
                $"[Consumer] Received: {message.BodyAsString()}");
            context.Accept();
            return Task.CompletedTask;
        })
        .BuildAndStartAsync()
        .ConfigureAwait(false);

    Console.WriteLine(
        "Consumer running. SingleActiveConsumerStateChanged fires when the broker marks this link active or standby " +
        "(RabbitMQ 4.3+). Press Enter to exit.");
    Console.ReadLine();

    await consumer.CloseAsync().ConfigureAwait(false);
    consumer.Dispose();
}
