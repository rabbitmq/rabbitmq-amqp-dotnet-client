using System.Diagnostics;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, a));


Trace.WriteLine(TraceLevel.Information, "Starting");
var connectionName = Guid.NewGuid().ToString();

var connection = await AmqpConnection.CreateAsync(
    ConnectionSettingBuilder.Create().ConnectionName(connectionName)
        .RecoveryConfiguration(RecoveryConfiguration.Create().Activated(true).Topology(true)).Build());

Trace.WriteLine(TraceLevel.Information, "Connected");
var management = connection.Management();
await management.Queue($"my-first-queue").Declare();
Trace.WriteLine(TraceLevel.Information, "Queue Created");
await management.QueueDeletion().Delete("my-first-queue");
Trace.WriteLine(TraceLevel.Information, "Queue Deleted");
Console.WriteLine("Press any key to close connection");
Console.ReadKey();
await connection.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Closed");