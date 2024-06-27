using System.Diagnostics;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using RabbitMQ.AMQP.Client;
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
    ConnectionSettingBuilder.Create().VirtualHost("aaa").ConnectionName(connectionName).RecoveryConfiguration(
        RecoveryConfiguration.Create().Activated(true).Topology(true)
    ).Build());

Trace.WriteLine(TraceLevel.Information, "Connected");

var management = connection.Management();
await management.Queue($"my-first-queue").Type(QueueType.QUORUM).Declare();

try
{
    var publisher = connection.PublisherBuilder().Queue("my-first-queue").Build();

//
    for (var i = 0; i < 500; i++)
    {
        try
        {
            await publisher.Publish(
                new AmqpMessage($"Hello World!{i}").MessageId($"id_{i}").CorrelationId(Guid.NewGuid().ToString())
                    .ReplyTo($"Reply_to{i}"),
                (message, descriptor) =>
                {
                    Console.WriteLine(
                        $"outcome result, state: {descriptor.State}, code: {descriptor.Code}, message_id: {message.MessageId()} Description: {descriptor.Description}, error: {descriptor.Error}");
                });
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Error, $"{e.Message}");
        }
    }

    await publisher.CloseAsync();
}
catch (Exception e)
{
    Trace.WriteLine(TraceLevel.Error, $"{e.Message}");
}

//
Trace.WriteLine(TraceLevel.Information, "Queue Created");
Console.WriteLine("Press any key to delete the queue and close the connection.");
Console.ReadKey();
await management.QueueDeletion().Delete("my-first-queue");
Trace.WriteLine(TraceLevel.Information, "Queue Deleted");
Console.ReadKey();
await connection.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Closed");