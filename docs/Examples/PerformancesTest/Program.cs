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
const string connectionName = "performance-test-connection";

IConnection connection = await AmqpConnection.CreateAsync(
    ConnectionSettingBuilder.Create().ConnectionName(connectionName).RecoveryConfiguration(
        RecoveryConfiguration.Create().Activated(true).Topology(true)
    ).Build()).ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, "Connected");

IManagement management = connection.Management();

await management.QueueDeletion().Delete("my-first-queue-n").ConfigureAwait(false);

await management.Queue($"my-first-queue-n").Type(QueueType.QUORUM).Declare().ConfigureAwait(false);

IPublisher publisher = await connection.PublisherBuilder().Queue("my-first-queue-n").MaxInflightMessages(2000).BuildAsync();

int received = 0;
DateTime start = DateTime.Now;

IConsumer consumer = await connection.ConsumerBuilder().Queue("my-first-queue-n").InitialCredits(1000).MessageHandler(
    (context, message) =>
    {
        received++;
        if (received % 200_000 == 0)
        {
            DateTime end = DateTime.Now;
            Console.WriteLine($"Received Time: {end - start} {received}");
        }

        context.Accept();
    }
).Stream().Offset(1).Builder().BuildAsync();

try
{
    int confirmed = 0;

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

            var message = new AmqpMessage(new byte[10]);
            PublishResult pr = await publisher.PublishAsync(message);
            if (pr.Outcome.State == OutcomeState.Accepted)
            {
                if (Interlocked.Increment(ref confirmed) % 200_000 != 0)
                {
                    return;
                }

                DateTime confirmEnd = DateTime.Now;
                Console.WriteLine($"Confirmed Time: {confirmEnd - start} {confirmed}");
            }
            else
            {
                Console.WriteLine(
                    $"outcome result, state: {pr.Outcome.State}, message_id: " +
                    $"{message.MessageId()}, error: {pr.Outcome.Error}");
            }
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
await publisher.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Publisher Closed");
await consumer.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Consumer Closed");
await management.QueueDeletion().Delete("my-first-queue-n").ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Queue Deleted");
await connection.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Closed");
