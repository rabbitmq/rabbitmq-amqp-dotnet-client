// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using System.Text;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Starting the example...");
const string containerId = "sql-filter-example";

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

// ------------------------------------------------------------------------------------
// The management object is used to declare/delete queues, exchanges, and bindings
IManagement management = connection.Management();
const string queueName = "q_amqp10-client-stream-filter-test";
IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.STREAM);
await queueSpec.DeclareAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information,
    $"Queue {queueName} declared successfully");
// ------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------

const int total = 10;
IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync().ConfigureAwait(false);
for (int i = 0; i < total; i++)
{
    string subject = i % 2 == 0 ? "Order" : "Invoice";
    string region = i % 2 == 0 ? "emea" : "us";
    IMessage message = new AmqpMessage(Encoding.UTF8.GetBytes($"Hello Filter {i}"))
        .Subject(subject)
        .Property("region", region);
    var pr = await publisher.PublishAsync(message).ConfigureAwait(false);

    switch (pr.Outcome.State)
    {
        case OutcomeState.Accepted:
            Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message: {message.BodyAsString()} confirmed");
            break;
        case OutcomeState.Released:
            Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message: {message.BodyAsString()} Released");
            break;

        case OutcomeState.Rejected:
            Trace.WriteLine(TraceLevel.Error,
                $"[Publisher] Message: {message.BodyAsString()} Rejected with error: {pr.Outcome.Error}");
            break;
        default:
            throw new ArgumentOutOfRangeException();
    }
}

// ------------------------------------------------------------------------------------
IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).MessageHandler((context, message) =>
        {
            Trace.WriteLine(TraceLevel.Information,
                $"[Consumer Received] Message Body: [{message.BodyAsString()}], " +
                $"Subject: [{message.Subject()}], Property: [{message.Property("region")}]");
            context.Accept();
            return Task.CompletedTask;
        }
    ).Stream().Offset(StreamOffsetSpecification.First).Filter().Subject("&p:Order").Property("region", "emea").Stream()
    .Builder()
    .BuildAndStartAsync().ConfigureAwait(false);
// close the above consumer to demonstrate SQL filter
Thread.Sleep(1000); // wait a bit to show the difference in output

// ------------------------------------------------------------------------------------
// the same like above but using SQL filter example

IConsumer sqlConsumer = await connection.ConsumerBuilder().Queue(queueName).MessageHandler((context, message) =>
        {
            Trace.WriteLine(TraceLevel.Information,
                $"[SQL Consumer Received] Message Body: [{message.BodyAsString()}], " +
                $"Subject: [{message.Subject()}], Property: [{message.Property("region")}]");
            context.Accept();
            return Task.CompletedTask;
        }
    ).Stream().Offset(StreamOffsetSpecification.First).Filter()
    .Sql("properties.subject LIKE 'Order%' AND region = 'emea'").Stream().Builder()
    .BuildAndStartAsync().ConfigureAwait(false);

Console.WriteLine("Press [enter] to exit.");
Console.ReadLine();
await consumer.CloseAsync().ConfigureAwait(false);
await sqlConsumer.CloseAsync().ConfigureAwait(false);
await publisher.CloseAsync().ConfigureAwait(false);
await queueSpec.DeleteAsync().ConfigureAwait(false);
await connection.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Connection closed");
