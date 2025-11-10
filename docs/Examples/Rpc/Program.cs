// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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

var recoveryConfiguration = new RecoveryConfiguration();
recoveryConfiguration.Topology(true);

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId)
        .RecoveryConfiguration(recoveryConfiguration)
        .Build());

IConnection connection = await environment.CreateConnectionAsync();
Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

IManagement management = connection.Management();

const string requestQueue = "amqp10.net-request-queue";

IQueueSpecification queueSpec = management.Queue(requestQueue).Type(QueueType.QUORUM);
await queueSpec.DeclareAsync();

const int messagesToSend = 10_000_000;
TaskCompletionSource<bool> tcs = new();
int messagesReceived = 0;
IResponder responder = await connection.ResponderBuilder().
    RequestQueue(requestQueue).Handler(
    (context, message) =>
    {
        try
        {
            Trace.WriteLine(TraceLevel.Information, $"[Server] Message received: {message.BodyAsString()} ");
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

IRequester requester = await connection.RequesterBuilder().RequestAddress().
        Queue(requestQueue).Requester().BuildAsync();

for (int i = 0; i < messagesToSend; i++)
{
    try
    {
        IMessage response = await requester.PublishAsync(
            new AmqpMessage($"ping_{DateTime.Now}"));
        Trace.WriteLine(TraceLevel.Information, $"[Client] Response received: {response.BodyAsString()}");
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

await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

await requester.CloseAsync();
await responder.CloseAsync();
await queueSpec.DeleteAsync();
await environment.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Bye!");
