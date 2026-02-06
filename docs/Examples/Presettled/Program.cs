// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// This client is a wrapper over the AMQP.Net Lite library:
// - It is meant to be used with RabbitMQ 4.x.
// - It provides an AMQP 1.0 implementation of the RabbitMQ concepts such as exchanges,
// queues, bindings, publishers, and consumers.
// - It provides management APIs to manage RabbitMQ topology via AMQP 1.0.
// - It provides connection recovery and publisher confirms.
// - It provides tracing and metrics capabilities.
// RabbitMQ AMQP 1.0 info: https://www.rabbitmq.com/docs/amqp
// RabbitMQ AMQP 1.0 .NET Example - Presettled Messages
// Full path example: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/tree/main/docs/Examples/Presettled/

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using IConnection = RabbitMQ.AMQP.Client.IConnection;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Presettled example...");
const string containerId = "presettled-id";

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

const string queueName = "presettled-queue";
IManagement management = connection.Management();
IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.QUORUM);
await queueSpec.DeclareAsync().ConfigureAwait(false);

IPublisher publisher = await connection.PublisherBuilder().Queue(queueName)
    .BuildAsync().ConfigureAwait(false);

const int total = 10;
for (int i = 0; i < total; i++)
{
    string body = $"Message {i}";
    Trace.WriteLine(TraceLevel.Information, $"[Publisher] Publishing presettled message: {body}");
    var message = new AmqpMessage(body);
    PublishResult result = await publisher.PublishAsync(message).ConfigureAwait(false);
    if (result.Outcome.State == OutcomeState.Accepted)
    {
        Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message {i} published successfully as presettled");
    }
    else
    {
        Trace.WriteLine(TraceLevel.Error,
            $"[Publisher] Message {i} failed to publish as presettled: {result.Outcome.Error}");
    }
}

// Short delay to observe the publishing process
await Task.Delay(TimeSpan.FromMilliseconds(200)).ConfigureAwait(false);

// create consumer to receive the presettled messages
IConsumer consumer = await connection.ConsumerBuilder()
    .Feature(ConsumerFeature.PreSettled) // indicate that messages are presettled. Like auto-acknowledge
    .Queue(queueName).MessageHandler((context, message) =>
        {
            Trace.WriteLine(TraceLevel.Information, $"[Consumer] Message: {message.BodyAsString()} received");
            // this message is presettled, so we just accept it, so you don't need to send disposition back to the broker
            // context.Accept(); is not necessary for presettled messages
            return Task.CompletedTask;
        }
    ).BuildAndStartAsync().ConfigureAwait(false);

// Short to see the consumer receiving messages
await Task.Delay(TimeSpan.FromMilliseconds(1000)).ConfigureAwait(false);
// cleanup
Console.WriteLine("Press any key to delete the queue and close the environment.");
Console.ReadKey();
await publisher.CloseAsync().ConfigureAwait(false);
await consumer.CloseAsync().ConfigureAwait(false);
await management.Queue(queueName).DeleteAsync().ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
