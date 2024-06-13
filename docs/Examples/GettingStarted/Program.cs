// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, a));


Trace.WriteLine(TraceLevel.Information, "Starting");
var connectionName = Guid.NewGuid().ToString();
AmqpConnection connection = new(
    new ConnectionSettingBuilder().
        ConnectionName(connectionName).
        RecoveryConfiguration(new RecoveryConfiguration().
            Activated(true).
            Topology(true)).
        Build());

await connection.ConnectAsync();
Trace.WriteLine(TraceLevel.Information, "Connected");

var management = connection.Management();
for (int i = 0; i < 50; i++)
{
    if (management.Status != Status.Open)
    {
        Trace.WriteLine(TraceLevel.Information, "Connection closed");
        Thread.Sleep(1000);
        continue;
    }

    try
    {
        await management.Queue($"re-recreate-queue_{i}").AutoDelete(true).Exclusive(true).Declare();
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
    }

    Trace.WriteLine(TraceLevel.Information, $"Queue {i} declared");
    await Task.Delay(2000);
}

Trace.WriteLine(TraceLevel.Information, "******************************************All queues declared");

// await management.Queue("re1-recreate-queue").AutoDelete(true).Exclusive(true).Declare();


Console.WriteLine("Press any key to close connection");
Console.ReadKey();
await connection.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Closed");