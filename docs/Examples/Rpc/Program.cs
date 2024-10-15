// See https://aka.ms/new-console-template for more information

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
    ConnectionSettingBuilder.Create().ContainerId(containerId).Build()).ConfigureAwait(false);

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

const string rpcQueue = "amqp10.net-rpc-queue";

IManagement management = connection.Management();

IQueueSpecification queueSpec = management.Queue(rpcQueue).Type(QueueType.QUORUM);
await queueSpec.DeclareAsync().ConfigureAwait(false);

const int messagesToSend = 10;
TaskCompletionSource<bool> tcs = new();
int messagesReceived = 0;
IRpcServer rpcServer = await connection.RpcServerBuilder().RequestQueue(rpcQueue).Handler(
    async (context, message) =>
    {
        try
        {
            Trace.WriteLine(TraceLevel.Information, $"[Server] Message received: {message.Body()} ");
            var reply = context.Message($"pong_{DateTime.Now}");
            return await Task.FromResult(reply).ConfigureAwait(false);
        }
        finally
        {
            if (Interlocked.Increment(ref messagesReceived) == messagesToSend)
            {
                tcs.SetResult(true);
            }
        }
    }
).BuildAsync().ConfigureAwait(false);

IRpcClient rpcClient = await connection.RpcClientBuilder().RequestAddress().Queue(rpcQueue).RpcClient().BuildAsync()
    .ConfigureAwait(false);

for (int i = 0; i < messagesToSend; i++)
{
    IMessage reply = await rpcClient.PublishAsync(
        new AmqpMessage($"ping_{DateTime.Now}")).ConfigureAwait(false);
    Trace.WriteLine(TraceLevel.Information, $"[Client] Reply received: {reply.Body()}");
    Thread.Sleep(500);
}

await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

await rpcClient.CloseAsync().ConfigureAwait(false);
await rpcServer.CloseAsync().ConfigureAwait(false);
await queueSpec.DeleteAsync().ConfigureAwait(false);
await environment.CloseAsync().ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, "Bye!");
