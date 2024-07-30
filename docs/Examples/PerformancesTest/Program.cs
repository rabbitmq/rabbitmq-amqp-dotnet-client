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
const string containerId = "performance-test-connection";

IEnvironment environment = await AmqpEnvironment
    .CreateAsync(ConnectionSettingBuilder.Create().ContainerId(containerId).Build()).ConfigureAwait(false);

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, "Connected");

IManagement management = connection.Management();

const string queueName = "amqp10-net-perf-test";
await management.QueueDeletion().Delete(queueName).ConfigureAwait(false);

await management.Queue(queueName).Type(QueueType.QUORUM).Declare().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Queue Created");

IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).MaxInflightMessages(5000).BuildAsync();

int received = 0;
DateTime start = DateTime.Now;

async Task MessageHandler(IContext context, IMessage message)
{
    await context.AcceptAsync();

    if (Interlocked.Increment(ref received) % 200_000 == 0)
    {
        DateTime end = DateTime.Now;
        Console.WriteLine($"Received Time: {end - start} {received}");
    }
};

IConsumer consumer = await connection.ConsumerBuilder()
    .Queue(queueName)
    .InitialCredits(1000)
    .MessageHandler(MessageHandler)
    .Stream().Offset(1).Builder().BuildAsync();

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

Console.WriteLine("Press any key to delete the queue and close the connection.");
Console.ReadKey();
await publisher.CloseAsync().ConfigureAwait(false);
await consumer.CloseAsync().ConfigureAwait(false);
await management.QueueDeletion().Delete(queueName).ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
