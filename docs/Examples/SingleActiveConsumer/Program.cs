// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// This client is a wrapper over the AMQP.Net Lite library:
// - It is meant to be used with RabbitMQ 4.x.
// - It provides an AMQP 1.0 implementation of the RabbitMQ concepts such as exchanges,
// queues, bindings, publishers, and consumers.
// RabbitMQ AMQP 1.0 info: https://www.rabbitmq.com/docs/amqp
// RabbitMQ AMQP 1.0 .NET Example - Single Active Consumer
// Full path example: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/tree/main/docs/Examples/SingleActiveConsumer
//
// Single Active Consumer (SAC): When enabled on a queue, only one consumer at a time receives
// messages from that queue. If the active consumer is cancelled or disconnects, the next
// consumer in line becomes active. This is useful for work distribution where you want
// exactly-one active consumer (e.g. ordered processing or avoiding duplicate work).

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Starting Single Active Consumer example...");
const string containerId = "single-active-consumer-connection";

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync();

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

// ------------------------------------------------------------------------------------
// Declare a queue with Single Active Consumer enabled.
// Only one consumer at a time will receive messages from this queue.
IManagement management = connection.Management();
const string queueName = "q_single-active-consumer-example";

IQueueSpecification queueSpec = management.Queue(queueName)
    .Type(QueueType.QUORUM)
    .SingleActiveConsumer(true);

await queueSpec.DeclareAsync();
Trace.WriteLine(TraceLevel.Information,
    $"Queue {queueName} declared with Single Active Consumer enabled (QUORUM type)");
// ------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------
// Declare a publisher (publishes directly to the queue) and a consumer.
IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).MessageHandler(
    (context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information, $"[Consumer] Message: {message.BodyAsString()} received");
        context.Accept();
        return Task.CompletedTask;
    }
).BuildAndStartAsync();
// ------------------------------------------------------------------------------------

IConsumer consumerNotActive = await connection.ConsumerBuilder().Queue(queueName).MessageHandler(
    (context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information,
            $"[ConsumerNotActive] Message: {message.BodyAsString()} received - this should not happen until the active consumer is cancelled or disconnects");
        context.Accept();
        return Task.CompletedTask;
    }
).BuildAndStartAsync();

const int total = 10;
for (int i = 0; i < total; i++)
{
    var message = new AmqpMessage($"Hello SAC_{i}");
    PublishResult pr = await publisher.PublishAsync(message);
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

Console.WriteLine("Press any key to delete queue and close the environment.");
Console.ReadKey();

await publisher.CloseAsync();
publisher.Dispose();

await consumer.CloseAsync();
consumer.Dispose();

await consumerNotActive.CloseAsync();
consumerNotActive.Dispose();

await queueSpec.DeleteAsync();

await environment.CloseAsync();

Trace.WriteLine(TraceLevel.Information, "Single Active Consumer example closed successfully");
