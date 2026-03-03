// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// This client is a wrapper over the AMQP.Net Lite library:
// - It is meant to be used with RabbitMQ 4.x.
// - It provides an AMQP 1.0 implementation of the RabbitMQ concepts such as exchanges,
// queues, bindings, publishers, and consumers.
// - It provides management APIs to manage RabbitMQ topology via AMQP 1.0.
// - It provides connection recovery and publisher confirms.
// - It provides tracing and metrics capabilities.
// RabbitMQ AMQP 1.0 info: https://www.rabbitmq.com/docs/amqp
// RabbitMQ AMQP 1.0 .NET Example - Node Affinity
// Full path example: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/tree/main/docs/Examples/Affinity

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
ConnectionSettings defaultSettings = ConnectionSettingsBuilder.Create().Uris([
    new Uri("amqp://localhost:5672"),
    new Uri("amqp://localhost:5673"),
    new Uri("amqp://localhost:5674")
]).Build();
IEnvironment environment = AmqpEnvironment.Create(defaultSettings);

// this connection is to create the queues, without affinity,
// so it can be created in any node of the cluster and then we can use it to test the affinity connection
IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);
IManagement management = connection.Management();
const string queueName = "q_amqp10-affinity-test";
for (int i = 0; i < 3; i++)
{
    IQueueSpecification queueSpec = management.Queue($"{queueName}_{i}").Type(QueueType.QUORUM);
    await queueSpec.DeclareAsync().ConfigureAwait(false);
}

const string containerId = "affinity-id-2";
const string queue1Name = $"{queueName}_1";
IConnection connectionAffinityQ1 = await environment
    .CreateConnectionAsync(ConnectionSettingsBuilder.From(defaultSettings).ContainerId(containerId)
        .Affinity(new DefaultAffinity("queue1Name", Operation.Publish)).Build()).ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connectionAffinityQ1} successfully");

// ------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------
IPublisher publisher = await connectionAffinityQ1.PublisherBuilder().Queue(queue1Name)
    .BuildAsync().ConfigureAwait(false);

IConsumer consumer = await connection.ConsumerBuilder().Queue(queue1Name).MessageHandler((context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information, $"[Consumer] Message: {message.BodyAsString()} received");
        context.Accept();
        return Task.CompletedTask;
    }
).BuildAndStartAsync().ConfigureAwait(false);
// ------------------------------------------------------------------------------------

const int total = 10;
for (int i = 0; i < total; i++)
{
    var message = new AmqpMessage($"Hello World_{i}");
    PublishResult pr = await publisher.PublishAsync(message).ConfigureAwait(false);
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

Console.WriteLine("Press any key to delete queue, exchange and close the environment.");
Console.ReadKey();

await publisher.CloseAsync().ConfigureAwait(false);
publisher.Dispose();

await consumer.CloseAsync().ConfigureAwait(false);
consumer.Dispose();

for (int i = 0; i < 3; i++)
{
    IQueueSpecification queueSpec = management.Queue($"{queueName}_{i}").Type(QueueType.QUORUM);
    await queueSpec.DeleteAsync().ConfigureAwait(false);
}

await environment.CloseAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, "Example closed successfully");
