using System.Diagnostics;
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

IConnection connection = await AmqpConnection.CreateAsync(
    ConnectionSettingBuilder.Create().ConnectionName(connectionName).RecoveryConfiguration(
        RecoveryConfiguration.Create().Activated(true).Topology(true)
    ).Build());

Trace.WriteLine(TraceLevel.Information, "Connected");

IManagement management = connection.Management();

await management.QueueDeletion().Delete("my-first-queue-n");

await management.Queue($"my-first-queue-n").Type(QueueType.QUORUM).Declare();

try
{
    IPublisher publisher = connection.PublisherBuilder().Queue("my-first-queue-n").MaxInflightMessages(2000).Build();
    int confirmed = 0;
    DateTime start = DateTime.Now;
    const int total = 1_000_000;
    for (int i = 0; i < total; i++)
    {
        try
        {
            if (i % 200_000 == 0)
            {
                DateTime endp = DateTime.Now;
                Console.WriteLine($"Sending Time: {endp - start} - messages {i}");
            }

            await publisher.Publish(
                new AmqpMessage(new byte[10]),
                (message, descriptor) =>
                {
                    if (descriptor.State == OutcomeState.Accepted)
                    {
                        if (Interlocked.Increment(ref confirmed) % 200_000 != 0)
                        {
                            return;
                        }

                        DateTime end = DateTime.Now;
                        Console.WriteLine($"Confirmed Time: {end - start} {confirmed}");
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

    DateTime end = DateTime.Now;
    Console.WriteLine($"Total Sent Time: {end - start}");
}
catch (Exception e)
{
    Trace.WriteLine(TraceLevel.Error, $"{e.Message}");
}

Trace.WriteLine(TraceLevel.Information, "Queue Created");
Console.WriteLine("Press any key to delete the queue and close the connection.");
Console.ReadKey();
await management.QueueDeletion().Delete("my-first-queue-n");
Trace.WriteLine(TraceLevel.Information, "Queue Deleted");
await connection.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Closed");
