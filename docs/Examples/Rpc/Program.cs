#pragma warning disable CA2007 // Consider calling ConfigureAwait on the awaited task

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
const string containerId = "rpc-example-connection";

IEnvironment environment = await AmqpEnvironment.CreateAsync(
    ConnectionSettingBuilder.Create().ContainerId(containerId)
        .RecoveryConfiguration(RecoveryConfiguration.Create().Topology(true))
        .Build()).ConfigureAwait(false);

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

const string rpcQueue = "amqp10.net-rpc-queue";

IManagement management = connection.Management();

IQueueSpecification queueSpec = management.Queue(rpcQueue).Type(QueueType.QUORUM);
await queueSpec.DeclareAsync().ConfigureAwait(false);

const int messagesToSend = 10_000_000;
TaskCompletionSource<bool> tcs = new();
int messagesReceived = 0;
IRpcServer rpcServer = await connection.RpcServerBuilder().RequestQueue(rpcQueue).Handler(
    (context, message) =>
    {
        try
        {
            Trace.WriteLine(TraceLevel.Information, $"[Server] Message received: {message.Body()} ");
            IMessage reply = context.Message($"pong_{DateTime.Now}");
            return Task.FromResult(reply);
        }
        finally
        {
            if (Interlocked.Increment(ref messagesReceived) == messagesToSend)
            {
                tcs.SetResult(true);
            }
        }
    }
).BuildAsync();

IRpcClient rpcClient = await connection.RpcClientBuilder().RequestAddress().Queue(rpcQueue).RpcClient().BuildAsync()
    .ConfigureAwait(false);

for (int i = 0; i < messagesToSend; i++)
{
    try
    {
        IMessage reply = await rpcClient.PublishAsync(
            new AmqpMessage($"ping_{DateTime.Now}")).ConfigureAwait(false);
        Trace.WriteLine(TraceLevel.Information, $"[Client] Reply received: {reply.Body()}");
    }
    catch (Exception e)
    {
        Trace.WriteLine(TraceLevel.Error, $"[Client] PublishAsync Error: {e.Message}");
    }
    finally
    {
        await Task.Delay(500);
    }
}

await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

await rpcClient.CloseAsync().ConfigureAwait(false);
await rpcServer.CloseAsync().ConfigureAwait(false);
await queueSpec.DeleteAsync().ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Bye!");
