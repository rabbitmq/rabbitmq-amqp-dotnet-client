// Consumer priority example.
// Two consumers are attached to the same queue: the broker delivers messages to the
// consumer with the highest priority (rabbitmq:priority) as long as it has credits.
// When the high priority consumer goes away, the low priority one receives the messages.

using System.Diagnostics;
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
    ConnectionSettingsBuilder.Create().ContainerId("consumer-priority-example").Build());

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

const string queueName = "q_amqp10-client-consumer-priority";
const int total = 10;

IManagement management = connection.Management();
IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.QUORUM);
await queueSpec.DeclareAsync().ConfigureAwait(false);

int lowCount = 0;
int highCount = 0;
int received = 0;
TaskCompletionSource allReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);

MessageHandler Handler(Action count, int expected) => (ctx, _) =>
{
    ctx.Accept();
    count();
    if (Interlocked.Increment(ref received) == expected)
    {
        allReceived.TrySetResult();
    }

    return Task.CompletedTask;
};

IConsumer lowPriorityConsumer = await connection.ConsumerBuilder()
    .Queue(queueName)
    .Priority(1)
    .MessageHandler(Handler(() => Interlocked.Increment(ref lowCount), total))
    .BuildAndStartAsync().ConfigureAwait(false);

IConsumer highPriorityConsumer = await connection.ConsumerBuilder()
    .Queue(queueName)
    .Priority(5)
    .MessageHandler(Handler(() => Interlocked.Increment(ref highCount), total))
    .BuildAndStartAsync().ConfigureAwait(false);

async Task PublishAsync()
{
    IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync()
        .ConfigureAwait(false);
    for (int i = 0; i < total; i++)
    {
        await publisher.PublishAsync(new AmqpMessage($"message_{i}")).ConfigureAwait(false);
    }

    await publisher.CloseAsync().ConfigureAwait(false);
}

await PublishAsync().ConfigureAwait(false);
await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information,
    $"Both consumers attached: high priority received {highCount}, low priority received {lowCount}");

await highPriorityConsumer.CloseAsync().ConfigureAwait(false);

received = 0;
allReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
await PublishAsync().ConfigureAwait(false);
await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information,
    $"High priority consumer closed: high priority received {highCount}, low priority received {lowCount}");

await lowPriorityConsumer.CloseAsync().ConfigureAwait(false);
await queueSpec.DeleteAsync().ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Example completed.");
