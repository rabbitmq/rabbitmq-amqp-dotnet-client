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
// to Run this example you need to have a RabbitMQ cluster
// with 3 nodes running locally and the queues created in the example should be mirrored across the cluster.
// run: make rabbitmq-cluster-start in this repository.
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
const string baseQname = "q_amqp10-affinity-test";
const string queue1Name = $"{baseQname}_1";

// create three queues with the same name on each node of the cluster,
// with quorum type to make sure they are mirrored across the cluster.
// In this example we use only the queue with suffix _1,
// but the other two queues are there to increase the chances of seeing the affinity in action,
// by having multiple queues with the same name across the cluster.
for (int i = 0; i < 3; i++)
{
    IQueueSpecification queueSpec = management.Queue($"{baseQname}_{i}").Type(QueueType.QUORUM);
    await queueSpec.DeclareAsync().ConfigureAwait(false);
}

IConnection publishConnectionAffinityQ1 = await environment
    .CreateConnectionAsync(ConnectionSettingsBuilder.From(defaultSettings)
        // Override the default URI selector with a custom one that implements a round-robin strategy
        // to select the next URI from the list for each new connection attempt.
        // This avoids where the default selector randomly selects the same URI.
        // Not strictly necessary, but it is a way to see another interesting feature of the client,
        // the ability to plug in custom URI selectors.
        .UriSelector(new SequentialSelector())
        .ContainerId("publisher-affinity-id-1")
        .Affinity(new DefaultAffinity(queue1Name, Operation.Publish)).Build()).ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {publishConnectionAffinityQ1} successfully");

// ------------------------------------------------------------------------------------
IPublisher publisher = await publishConnectionAffinityQ1.PublisherBuilder().Queue(queue1Name)
    .BuildAsync().ConfigureAwait(false);

// ------------------------------------------------------------------------------------
IConnection consumeConnectionAffinityQ1 = await environment
    .CreateConnectionAsync(ConnectionSettingsBuilder.From(defaultSettings)
        .UriSelector(new SequentialSelector())
        .ContainerId("consumer-affinity-id-1")
        .Affinity(new DefaultAffinity(queue1Name, Operation.Consume)).Build()).ConfigureAwait(false);

// in case of cluster the consumer connection should go in a replica node, not the leader node,
IConsumer consumer = await consumeConnectionAffinityQ1.ConsumerBuilder().Queue(queue1Name).MessageHandler((context, message) =>
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
    IQueueSpecification queueSpec = management.Queue($"{baseQname}_{i}").Type(QueueType.QUORUM);
    await queueSpec.DeleteAsync().ConfigureAwait(false);
}

await environment.CloseAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, "Example closed successfully");

internal class SequentialSelector : IUriSelector
{
    private int _lastIndex = -1;

    public Uri Select(ICollection<Uri> uris)
    {
        if (uris == null || uris.Count == 0)
        {
            throw new ArgumentException("URIs collection cannot be null or empty.");
        }

        _lastIndex = (_lastIndex + 1) % uris.Count;
        return uris.ElementAt(_lastIndex);
    }
}
