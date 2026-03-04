
// AMQP 1.0 over WebSocket Example using the RabbitMQ AMQP Client Library
// This example demonstrates how to connect to a RabbitMQ broker using AMQP 1.0 over WebSocket,
// publish messages to a queue, and consume messages from that queue.

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;
Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Starting the websocket example...");

// Create a ws in rabbitmq in the virtual host ws and user rabbit with password rabbit

const string amqpConnectionString = "ws://rabbit:rabbit@127.0.0.1:15678/ws";
var cs = new ConnectionSettings(new Uri(amqpConnectionString));
IEnvironment environment = AmqpEnvironment.Create(cs);

// open a connection from the environment setting
IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);
IManagement management = connection.Management();
await management.Queue("ws-queue").Type(QueueType.QUORUM).DeclareAsync().ConfigureAwait(false);

// publish a message
IPublisher publisher = await connection.PublisherBuilder().Queue("ws-queue").BuildAsync().ConfigureAwait(false);
for (int i = 0; i < 10; i++)
{
    PublishResult r = await publisher.PublishAsync(new AmqpMessage("Hello over WebSocket " + i)).ConfigureAwait(false);
    switch (r.Outcome.State)
    {
        case OutcomeState.Accepted:
            Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message {i} published successfully over WebSocket");
            break;
        case OutcomeState.Rejected:
            Trace.WriteLine(TraceLevel.Error, $"[Publisher] Message {i} was rejected over WebSocket: {r.Outcome.Error}");
            break;
        case OutcomeState.Released:
            Trace.WriteLine(TraceLevel.Warning, $"[Publisher] Message {i} was Released over WebSocket");
            break;

    }

    await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
}

// consume messages
IConsumer consumer = await connection.ConsumerBuilder().Queue("ws-queue").MessageHandler(
    (context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information, $"[Consumer] Message: {message.BodyAsString()} received over WebSocket");
        context.Accept();
        return Task.CompletedTask;
    }
).BuildAndStartAsync().ConfigureAwait(false);

// press a key to close

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");
Trace.WriteLine(TraceLevel.Information, "Press any key to exit...");
Console.ReadKey();
await management.Queue("ws-queue").DeleteAsync().ConfigureAwait(false);
await consumer.CloseAsync().ConfigureAwait(false);
await publisher.CloseAsync().ConfigureAwait(false);
await connection.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Closed connection. Exiting...");

