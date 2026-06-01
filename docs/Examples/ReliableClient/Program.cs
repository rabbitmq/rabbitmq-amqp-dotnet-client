// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// This client is a wrapper over the AMQP.Net Lite library:
// - It is meant to be used with RabbitMQ 4.x.
// - It provides an AMQP 1.0 implementation of the RabbitMQ concepts such as exchanges,
// queues, bindings, publishers, and consumers.
// - It provides management APIs to manage RabbitMQ topology via AMQP 1.0.
// - It provides connection recovery and publisher confirms.
// - It provides tracing and metrics capabilities.
// RabbitMQ AMQP 1.0 info: https://www.rabbitmq.com/docs/amqp
// RabbitMQ AMQP 1.0 .NET Example - High Availability Client. The example creates provides a guide-line
// to deal with high availability scenarios by monitoring connection and link states
// Full path example: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/tree/main/docs/Examples/ReliableClient

using System.Globalization;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;
Trace.TraceListener = (l, f, a) => ConsoleEx.WriteTrace(l, f, a);

long messagesReceived = 0;
long messagesConfirmed = 0;
long notMessagesConfirmed = 0;
long messagesFailed = 0;

const int totalMessagesToSend = 5_000_000;

Task printStats = Task.Run(() =>
{
    while (true)
    {
        ConsoleEx.WriteStats(
            $"[(Confirmed: {Interlocked.Read(ref messagesConfirmed)}, " +
            $"Failed: {Interlocked.Read(ref messagesFailed)}, UnConfirmed: {Interlocked.Read(ref notMessagesConfirmed)} )] " +
            $"[(Received: {Interlocked.Read(ref messagesReceived)})] " +
            $"(Un/Confirmed+Failed : {messagesConfirmed + messagesFailed + notMessagesConfirmed} ) ");
        Thread.Sleep(1000);
    }
});

Trace.WriteLine(TraceLevel.Information, "Starting");
const string containerId = "HA-Client-Connection";

ConnectionSettings defaultSettings = ConnectionSettingsBuilder.Create().ContainerId(containerId)
    .Build();

IEnvironment environment = AmqpEnvironment.Create(defaultSettings);

IConnection connection = await environment.CreateConnectionAsync();

connection.ChangeState += (sender, fromState, toState, e) =>
    ConsoleEx.WriteLifecycle("Connection", fromState, toState);

Trace.WriteLine(TraceLevel.Information, "Connected");

IManagement management = connection.Management();
const string queueName = "ha-amqp10-client-test";
IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.QUORUM);

await queueSpec.DeleteAsync();
await queueSpec.DeclareAsync();

IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

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

IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).InitialCredits(100)
    .MessageHandler((context, message) =>
        {
            Interlocked.Increment(ref messagesReceived);
            context.Accept();
            return Task.CompletedTask;
        }
    ).BuildAndStartAsync();

consumer.ChangeState += (sender, fromState, toState, e) =>
    ConsoleEx.WriteLifecycle("Consumer", fromState, toState);

for (int i = 0; i < totalMessagesToSend; i++)
{
    try
    {
        // TODO
        // Use a cancellation token for a timeout and for
        // CTRL-C handling
        pausePublishing.WaitOne(TimeSpan.FromMinutes(1));
        var message = new AmqpMessage($"Hello World_{i}");
        PublishResult pr = await publisher.PublishAsync(message);
        if (pr.Outcome.State == OutcomeState.Accepted)
        {
            Interlocked.Increment(ref messagesConfirmed);
        }
        else
        {
            Interlocked.Increment(ref notMessagesConfirmed);
        }
    }
    catch (Exception e)
    {
        Trace.WriteLine(TraceLevel.Error, $"Failed to publish message, {e.Message}");
        Interlocked.Increment(ref messagesFailed);
        await Task.Delay(500);
    }
    finally
    {
    }
}

Trace.WriteLine(TraceLevel.Information, "Queue Created");
ConsoleEx.WritePrompt("Press any key to delete the queue and close the connection.");
Console.ReadKey();

await publisher.CloseAsync();
publisher.Dispose();

await consumer.CloseAsync();
consumer.Dispose();

await queueSpec.DeleteAsync();

await connection.CloseAsync();
connection.Dispose();

printStats.Dispose();
Trace.WriteLine(TraceLevel.Information, "Closed");

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

    private static ConsoleColor ColorForState(State state) => state switch
    {
        State.Open => ConsoleColor.Green,
        State.Reconnecting => ConsoleColor.Yellow,
        State.Closing => ConsoleColor.DarkYellow,
        State.Closed => ConsoleColor.DarkGray,
        _ => ConsoleColor.Gray
    };
}
