// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// RabbitMQ AMQP 1.0 .NET Example - Delayed messages on a quorum queue with IMessage.DeliveryAfter()
// Docs: https://www.rabbitmq.com/docs/quorum-queues#delayed-messages
// Full path example: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/tree/main/docs/Examples/QQDeliveryAfter/

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;
ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now:HH:mm:ss}] [{l}] - {f}");

const string queueName = "qq-delivery-after-queue";
const int total = 3;
TimeSpan delay = TimeSpan.FromSeconds(10);

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId("qq-delivery-after-example").Build());
IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

IManagement management = connection.Management();
await management.Queue(queueName).Type(QueueType.QUORUM).DeclareAsync().ConfigureAwait(false);

IPublisher publisher = await connection.PublisherBuilder().Queue(queueName)
    .BuildAsync().ConfigureAwait(false);

// The broker holds each message back until now + delay.
// DeliveryAfter sets the x-opt-delivery-time annotation (ms since the Unix epoch).
for (int i = 0; i < total; i++)
{
    IMessage message = new AmqpMessage($"message {i}").DeliveryAfter(delay);
    PublishResult result = await publisher.PublishAsync(message).ConfigureAwait(false);
    Trace.WriteLine(TraceLevel.Information,
        $"[Publisher] message {i} published ({result.Outcome.State}), it will be delivered in {delay.TotalSeconds}s");
}

CountdownEvent received = new(total);
IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName)
    .MessageHandler((context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information, $"[Consumer] received {message.BodyAsString()}");
        context.Accept();
        received.Signal();
        return Task.CompletedTask;
    }).BuildAndStartAsync().ConfigureAwait(false);

received.Wait(delay + TimeSpan.FromSeconds(30));

Console.WriteLine("Press any key to delete the queue and close the environment.");
Console.ReadKey();
await publisher.CloseAsync().ConfigureAwait(false);
await consumer.CloseAsync().ConfigureAwait(false);
await management.Queue(queueName).DeleteAsync().ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
