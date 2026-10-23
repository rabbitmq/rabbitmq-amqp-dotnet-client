// Stream initial offset example (requires RabbitMQ 4.4 or later).
// The first message published to the stream gets the configured initial offset
// (x-stream-initial-offset) instead of 0.

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

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId("stream-initial-offset-example").Build());

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

const string queueName = "q_amqp10-client-stream-initial-offset";
const long initialOffset = 1000;
const int total = 5;

IManagement management = connection.Management();
try
{
    await management.Queue(queueName).Stream().InitialOffset(initialOffset).Queue()
        .DeclareAsync().ConfigureAwait(false);
}
catch (NotSupportedException e)
{
    Trace.WriteLine(TraceLevel.Error, e.Message);
    await environment.CloseAsync().ConfigureAwait(false);
    return;
}

Trace.WriteLine(TraceLevel.Information, $"Stream {queueName} declared with initial offset {initialOffset}");

IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync().ConfigureAwait(false);
for (int i = 0; i < total; i++)
{
    await publisher.PublishAsync(new AmqpMessage(Encoding.UTF8.GetBytes($"Hello {i}"))).ConfigureAwait(false);
}

TaskCompletionSource<bool> allReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
int received = 0;

IConsumer consumer = await connection.ConsumerBuilder()
    .Queue(queueName)
    .MessageHandler((context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information,
            $"[Consumer] Body: [{message.BodyAsString()}], x-stream-offset: [{message.Annotation("x-stream-offset")}]");
        context.Accept();
        if (Interlocked.Increment(ref received) == total)
        {
            allReceived.TrySetResult(true);
        }

        return Task.CompletedTask;
    })
    .Stream().Offset(StreamOffsetSpecification.First).Builder()
    .BuildAndStartAsync().ConfigureAwait(false);

await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

await consumer.CloseAsync().ConfigureAwait(false);
await publisher.CloseAsync().ConfigureAwait(false);
await management.Queue(queueName).DeleteAsync().ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Done");
