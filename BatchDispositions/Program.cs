// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Amqp;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using IConnection = RabbitMQ.AMQP.Client.IConnection;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Starting the example...");
const string containerId = "dispositions-id";

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

IManagement management = connection.Management();
const string queueName = "dispositions";
IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.QUORUM);
await queueSpec.DeclareAsync().ConfigureAwait(false);

IPublisher publisher = await connection.PublisherBuilder().Queue(queueName)
    .BuildAsync().ConfigureAwait(false);

BatchDeliveryContext batchContext = new();
IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).MessageHandler(
    (context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information, $"[Consumer] Message: {message.BodyAsString()} received");
        batchContext.Add(context);
        if (batchContext.Count() >= 10)
        {
            Trace.WriteLine(TraceLevel.Information, "[Consumer] Committing batch of messages");
            // here the batch is committed, all messages in the batch will be accepted
            // the contexts will be disposed and deleted after the batch is committed
            batchContext.Accept();
        }
        else
        {
            Trace.WriteLine(TraceLevel.Information, "[Consumer] Adding message to batch");
        }
        return Task.CompletedTask;
    }
).BuildAndStartAsync().ConfigureAwait(false);

const int total = 10;
for (int i = 0; i < total; i++)
{
    string body = $"Message {i}";
    Trace.WriteLine(TraceLevel.Information, $"[Publisher] Publishing message: {body}");
    var message = new AmqpMessage($"Hello World_{i}");
    await publisher.PublishAsync(message).ConfigureAwait(false);
    // ignoring the publish result for this example
}

Console.WriteLine("Press any key to delete the queue and close the environment.");
Console.ReadKey();

await publisher.CloseAsync().ConfigureAwait(false);
await consumer.CloseAsync().ConfigureAwait(false);
await queueSpec.DeleteAsync().ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
