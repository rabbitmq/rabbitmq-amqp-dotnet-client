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
    ConnectionSettingBuilder.Create().ConnectionName(connectionName).RecoveryConfiguration(
        RecoveryConfiguration.Create().Activated(true).Topology(true)
    ).Build());

Trace.WriteLine(TraceLevel.Information, "Connected");

var management = connection.Management();
await management.QueueDeletion().Delete("my-first-queue-n");
await management.Queue($"my-first-queue-n").Type(QueueType.QUORUM).Declare();


try
{
    var publisher = connection.PublisherBuilder().Queue("my-first-queue-n").Build();
    // var c = new Connection(new Address("amqp://localhost:5672"));
    // var s = new Session(c);
    // var sender = new SenderLink(s, "sender-link", "/queue/my-first-queue-n");
    //
    var confirmed = 0;

    var start = DateTime.Now;
    const int total = 5_000_000;
    for (var i = 0; i < total; i++)
    {
        try
        {
            if (i % 200_000 == 0)
            {
                var endp = DateTime.Now;
                Console.WriteLine($"Sending Time: {endp - start} - messages {i}");
            }

            // sender.Send(new Message(new byte[10]), (link, message, outcome, state) => { }, null);

            await publisher.Publish(
                new AmqpMessage(new byte[10]),
                (message, descriptor) =>
                {
                    if (descriptor.State == OutcomeState.Accepted)
                    {
                        if (Interlocked.Increment(ref confirmed) % 200_000 != 0) return;
                        var end = DateTime.Now;
                        Console.WriteLine($"Confirmed Time: {end - start} {confirmed}");


                        // Console.WriteLine(
                        // $"outcome result, state: {descriptor.State}, code: {descriptor.Code}, message_id: {message.MessageId()} Description: {descriptor.Description}, error: {descriptor.Error}");
                    }
                    else
                    {
                        Console.WriteLine(
                        $"outcome result, state: {descriptor.State}, code: {descriptor.Code}, message_id: " +
                            $"{message.MessageId()} Description: {descriptor.Description}, error: {descriptor.Error}");
                    }

                });
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Error, $"{e.Message}");
        }
    }

    var end = DateTime.Now;
    Console.WriteLine($"Total Sent Time: {end - start}");
}
catch (Exception e)
{
    Trace.WriteLine(TraceLevel.Error, $"{e.Message}");
}

//
Trace.WriteLine(TraceLevel.Information, "Queue Created");
Console.WriteLine("Press any key to delete the queue and close the connection.");
// await publisher.CloseAsync();
Console.ReadKey();
await management.QueueDeletion().Delete("my-first-queue-n");
Trace.WriteLine(TraceLevel.Information, "Queue Deleted");
Console.ReadKey();
await connection.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Closed");