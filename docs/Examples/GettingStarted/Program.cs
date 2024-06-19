using System.Diagnostics;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Verbose;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");


Trace.WriteLine(TraceLevel.Information, "Starting");
const string connectionName = "Hello-Connection";

var connection = await AmqpConnection.CreateAsync(
    ConnectionSettingBuilder.Create().
        ConnectionName(connectionName)
        .RecoveryConfiguration(
            RecoveryConfiguration.Create().
                Activated(true).Topology(true)
            ).Build());

Trace.WriteLine(TraceLevel.Information, "Connected");

var management = connection.Management();
await management.Queue($"my-first-queue").
    AutoDelete(true).Exclusive(true).Declare();

Trace.WriteLine(TraceLevel.Information, "Queue Created");
Console.WriteLine("Press any key to delete the queue and close the connection.");
Console.ReadKey();
await management.QueueDeletion().Delete("my-first-queue");
Trace.WriteLine(TraceLevel.Information, "Queue Deleted");
await connection.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Closed");