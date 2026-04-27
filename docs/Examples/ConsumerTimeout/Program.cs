// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// RabbitMQ AMQP 1.0 client: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client
// RabbitMQ AMQP 1.0 .NET Example - Quorum queue consumer timeout (x-consumer-timeout)
// Full path example: https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/tree/main/docs/Examples/ConsumerTimeout
//
// Consumer timeouts limit how long a consumer may hold a message without settling it (accept / reject / release).
// For quorum queues you can set:
// - Queue argument x-consumer-timeout (via IQueueSpecification.Quorum().ConsumerTimeout(...))
// - Per-consumer attach property rabbitmq:consumer-timeout (via IConsumerBuilder.Quorum().ConsumerTimeout(...).Builder())
// See: https://www.rabbitmq.com/docs/consumers#consumer-timeout
//
// This sample declares a quorum queue with a queue-level timeout, attaches a consumer with its own consumer-level
// timeout (highest precedence when both are set), and settles messages immediately so the timeout is not hit.

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Starting ConsumerTimeout (quorum queue) example...");
const string containerId = "consumer-timeout-example-connection";

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingsBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync();
Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

IManagement management = connection.Management();
const string queueName = "q_amqp10-consumer-timeout-example";

// TimeSpan queueConsumerTimeout = TimeSpan.FromMinutes(3);
TimeSpan attachConsumerTimeout = TimeSpan.FromSeconds(3);

IQueueSpecification queueSpec = management.Queue(queueName)
    .Quorum()
    // .ConsumerTimeout(queueConsumerTimeout)
    .Queue();

IQueueInfo queueInfo = await queueSpec.DeclareAsync();
// Trace.WriteLine(TraceLevel.Information,
//     $"Queue declared with x-consumer-timeout={queueInfo.Arguments()["x-consumer-timeout"]} ms (queue default).");

IPublisher publisher = await connection.PublisherBuilder().Queue(queueSpec).BuildAsync();

IConsumer consumer = await connection.ConsumerBuilder()
    .Queue(queueSpec)
    .Quorum()
    .ConsumerTimeout(attachConsumerTimeout)
    .Builder()

    .MessageHandler(async (context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information,
            $"[Consumer] Message: {message.BodyAsString()} received; settling within consumer timeout");
        await Task.Delay(TimeSpan.FromSeconds(4));
        Trace.WriteLine(TraceLevel.Information,
            $"[Consumer] Message: {message.BodyAsString()} after delay");
        context.Accept();
    })
    .BuildAndStartAsync();

Trace.WriteLine(TraceLevel.Information,
    $"Consumer attached with rabbitmq:consumer-timeout={(uint)attachConsumerTimeout.TotalMilliseconds} ms (per-consumer).");

const int total = 0;
for (int i = 0; i < total; i++)
{
    var message = new AmqpMessage($"consumer-timeout-demo_{i}");
    PublishResult pr = await publisher.PublishAsync(message);
    switch (pr.Outcome.State)
    {
        case OutcomeState.Accepted:
            Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message confirmed: {message.BodyAsString()}");
            break;
        case OutcomeState.Released:
            Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message released: {message.BodyAsString()}");
            break;
        case OutcomeState.Rejected:
            Trace.WriteLine(TraceLevel.Error,
                $"[Publisher] Message rejected: {message.BodyAsString()} error={pr.Outcome.Error}");
            break;
        default:
            throw new ArgumentOutOfRangeException();
    }
}

Console.WriteLine("Press any key to close publisher, consumer, delete the queue, and exit.");
Console.ReadKey();

await publisher.CloseAsync();
publisher.Dispose();

await consumer.CloseAsync();
consumer.Dispose();

await queueSpec.DeleteAsync();

await environment.CloseAsync();

Trace.WriteLine(TraceLevel.Information, "Example closed successfully");
