// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// This client is a wrapper over the AMQP.Net Lite library:
// - It is meant to be used with RabbitMQ 4.x.
// - It provides an AMQP 1.0 implementation of the RabbitMQ concepts such as exchanges,
// queues, bindings, publishers, and consumers.
// - It provides management APIs to manage RabbitMQ topology via AMQP 1.0.
// - It provides connection recovery and publisher confirms.
// - It provides tracing and metrics capabilities.
// RabbitMQ AMQP 1.0 info: https://www.rabbitmq.com/docs/amqp
// RabbitMQ AMQP 1.0 stream consumer filter on the AMQP "group-id" message property.
//
// Usage:
//   dotnet run -- producer <groupId>
//   dotnet run -- consumer <groupId>
//
// Typical flow: start the consumer, then the producer (consumer uses offset "first"
// so it also works if the producer ran first for this queue).

using System.Diagnostics;
using System.Text;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

if (args.Length < 2)
{
    Console.Error.WriteLine("Usage: GroupIdStreamFiltering <producer|consumer> <groupId>");
    return 1;
}

string mode = args[0].Trim().ToLowerInvariant();
string groupId = args[1];
if (string.IsNullOrWhiteSpace(groupId))
{
    Console.Error.WriteLine("groupId must be a non-empty string.");
    return 1;
}

if (mode is not ("producer" or "consumer"))
{
    Console.Error.WriteLine("First argument must be \"producer\" or \"consumer\".");
    return 1;
}

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

const string queueName = "q_amqp10-client-group-id-stream-filter-example";
string containerId = $"group-id-stream-filtering-{mode}";

Trace.WriteLine(TraceLevel.Information, "Starting the example...");

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

try
{
    if (mode == "producer")
    {
        await RunProducerAsync(connection, queueName, groupId).ConfigureAwait(false);
    }
    else
    {
        await RunConsumerAsync(connection, queueName, groupId).ConfigureAwait(false);
    }
}
finally
{
    await connection.CloseAsync().ConfigureAwait(false);
    Trace.WriteLine(TraceLevel.Information, "Connection closed");
}

return 0;

static async Task RunProducerAsync(IConnection connection, string queueName, string groupId)
{
    IManagement management = connection.Management();
    IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.STREAM).MaxLengthBytes(ByteCapacity.B(500));
    await queueSpec.DeclareAsync().ConfigureAwait(false);
    Trace.WriteLine(TraceLevel.Information, $"Queue {queueName} declared successfully");

    IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync().ConfigureAwait(false);

    const int total = 10000;
    for (int i = 0; i < total; i++)
    {
        await Task.Delay(100).ConfigureAwait(false);
        IMessage message = new AmqpMessage(Encoding.UTF8.GetBytes($"Hello group [{groupId}] #{i}"))
            .GroupId(groupId);
        PublishResult pr = await publisher.PublishAsync(message).ConfigureAwait(false);

        switch (pr.Outcome.State)
        {
            case OutcomeState.Accepted:
                Trace.WriteLine(TraceLevel.Information,
                    $"[Publisher] Message: {message.BodyAsString()} confirmed (group-id: {message.GroupId()})");
                break;
            case OutcomeState.Released:
                Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message: {message.BodyAsString()} released");
                break;
            case OutcomeState.Rejected:
                Trace.WriteLine(TraceLevel.Error,
                    $"[Publisher] Message: {message.BodyAsString()} rejected with error: {pr.Outcome.Error}");
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    await publisher.CloseAsync().ConfigureAwait(false);
}

static async Task RunConsumerAsync(IConnection connection, string queueName, string groupId)
{
    IManagement management = connection.Management();
    IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.STREAM).MaxLengthBytes(ByteCapacity.B(500));
    await queueSpec.DeclareAsync().ConfigureAwait(false);
    Trace.WriteLine(TraceLevel.Information, $"Queue {queueName} declared successfully");

    IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).MessageHandler((context, message) =>
        {

            Trace.WriteLine(TraceLevel.Information,
                $"[Consumer] Body: [{message.BodyAsString()}], group-id: [{message.GroupId()}], offset: [{message.Annotation("x-stream-offset")}]");
            context.Accept();
            return Task.CompletedTask;
        }
    ).Stream().Offset(StreamOffsetSpecification.First).Filter().GroupId(groupId).Stream()
    .Builder()

    // .SubscriptionListener(context =>
    // {
    //    // in case you want to restart from an specific offset   
    //    // Offset is in: message.Annotation("x-stream-offset")
    //     context.StreamOptions.Offset(100);
    //
    // } )
    .BuildAndStartAsync().ConfigureAwait(false);

    Trace.WriteLine(TraceLevel.Information,
        $"Consumer started; filter expression: group-id = \"{groupId}\". Press [enter] to exit.");
    Console.ReadLine();

    await consumer.CloseAsync().ConfigureAwait(false);
}
