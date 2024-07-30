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


IEnvironment environment = await AmqpEnvironment
    .CreateAsync(ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build()).ConfigureAwait(false);

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, "Connected");

IManagement management = connection.Management();

const string queueName = "amqp10-net-perf-test";
await management.QueueDeletion().Delete(queueName).ConfigureAwait(false);

await management.Queue(queueName).Type(QueueType.QUORUM).Declare().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Queue Created");
IPublisher publisher = connection.PublisherBuilder().Queue(queueName).MaxInflightMessages(5000).Build();
int received = 0;
DateTime start = DateTime.Now;

IConsumer consumer = connection.ConsumerBuilder().Queue(queueName).InitialCredits(1000).MessageHandler(
    async (context, message) =>
    {
        await context.Accept().ConfigureAwait(false);
        if (Interlocked.Increment(ref received) % 200_000 != 0)
        {
            return;
        }

        DateTime end = DateTime.Now;
        Console.WriteLine($"Received Time: {end - start} {received}");
    }
).Stream().Offset(1).Builder().Build();

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
                }).ConfigureAwait(false);
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

Console.WriteLine("Press any key to delete the queue and close the connection.");
Console.ReadKey();
await publisher.CloseAsync().ConfigureAwait(false);
await consumer.CloseAsync().ConfigureAwait(false);
await management.QueueDeletion().Delete(queueName).ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
