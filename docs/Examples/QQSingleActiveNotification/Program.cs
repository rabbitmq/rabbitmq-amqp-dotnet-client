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

using System.Globalization;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

if (args.Length < 1)
{
    ConsoleEx.WriteError("Usage: QQSingleActiveNotification <producer|consumer>");
    return 1;
}

string mode = args[0].Trim().ToLowerInvariant();
const string queueName = "demo-qq-sac-notification"; // default queue name

if (mode is not ("producer" or "consumer"))
{
    ConsoleEx.WriteError("First argument (type) must be \"producer\" or \"consumer\".");
    return 1;
}

Trace.TraceLevel = TraceLevel.Information;
Trace.TraceListener = (l, f, a) => ConsoleEx.WriteTrace(l, f, a);

string containerId = $"qq-sac-notification-{mode}-{Environment.ProcessId}";

Trace.WriteLine(TraceLevel.Information, "Starting QQ Single Active Consumer notification example...");

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId)
        .Build());

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
    ManualResetEvent pausePublishing = new(true);
    publisher.ChangeState += (sender, fromState, toState, e) =>
    {
        ConsoleEx.WriteLifecycle("Publisher", fromState, toState);

        if (toState == State.Open)
        {
            pausePublishing.Set();
        }
        else
        {
            pausePublishing.Reset();
        }
    };


    const int total = 3000;
    for (int i = 0; i < total; i++)
    {
        pausePublishing.WaitOne();
        await Task.Delay(500).ConfigureAwait(false);
        var message = new AmqpMessage($"SAC demo message #{i}");

        try
        {
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
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Error, $"Failed to publish message, {e.Message}");
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
        .SingleActiveConsumerStateChanged((c, isActive) => ConsoleEx.WriteSacNotification(isActive, containerId))
        .Builder()
        .MessageHandler((context, message) =>
        {
            Trace.WriteLine(TraceLevel.Information,
                $"[Consumer] Received: {message.BodyAsString()}");
            context.Accept();
            return Task.CompletedTask;
        })
        .BuildAndStartAsync()
        .ConfigureAwait(false);

    ConsoleEx.WritePrompt(
        "Consumer running. SingleActiveConsumerStateChanged fires when the broker marks this link active or standby " +
        "(RabbitMQ 4.3+). Press Enter to exit.");
    Console.ReadLine();

    await consumer.CloseAsync().ConfigureAwait(false);
    consumer.Dispose();
}

file static class ConsoleEx
{
    private static readonly object s_consoleLock = new();

    public static void WriteTrace(TraceLevel level, string format, object[]? args)
    {
        string text = args is { Length: > 0 }
            ? string.Format(CultureInfo.InvariantCulture, format, args)
            : format;

        lock (s_consoleLock)
        {
            ConsoleColor previous = Console.ForegroundColor;
            Console.ForegroundColor = level switch
            {
                TraceLevel.Error => ConsoleColor.Red,
                TraceLevel.Warning => ConsoleColor.Yellow,
                TraceLevel.Information => ConsoleColor.DarkCyan,
                TraceLevel.Verbose => ConsoleColor.DarkGray,
                _ => ConsoleColor.Gray
            };
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{level}] - {text}");
            Console.ForegroundColor = previous;
        }
    }

    public static void WriteLifecycle(string label, State fromState, State toState)
    {
        lock (s_consoleLock)
        {
            ConsoleColor previous = Console.ForegroundColor;
            Console.ForegroundColor = ColorForState(toState);
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{label}] {fromState} → {toState}");
            Console.ForegroundColor = previous;
        }
    }

    public static void WriteStats(string line)
    {
        lock (s_consoleLock)
        {
            ConsoleColor previous = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.DarkCyan;
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [stats] {line}");
            Console.ForegroundColor = previous;
        }
    }

    public static void WritePrompt(string message)
    {
        lock (s_consoleLock)
        {
            ConsoleColor previous = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(message);
            Console.ForegroundColor = previous;
        }
    }

    public static void WriteError(string message)
    {
        lock (s_consoleLock)
        {
            ConsoleColor previous = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Error.WriteLine(message);
            Console.ForegroundColor = previous;
        }
    }

    public static void WriteSacNotification(bool isActive, string containerId)
    {
        lock (s_consoleLock)
        {
            string state = isActive ? "ACTIVE (this link delivers messages)" : "STANDBY";
            ConsoleColor previous = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("********************************");
            Console.ForegroundColor = isActive ? ConsoleColor.Green : ConsoleColor.DarkYellow;
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] Single Active Consumer state: {state} containerId={containerId}");
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("********************************");
            Console.ForegroundColor = previous;
        }
    }

    private static ConsoleColor ColorForState(State state) => state switch
    {
        State.Open => ConsoleColor.Green,
        State.Reconnecting => ConsoleColor.Yellow,
        State.Closing => ConsoleColor.DarkYellow,
        State.Closed => ConsoleColor.DarkGray,
        _ => ConsoleColor.Gray
    };
}
