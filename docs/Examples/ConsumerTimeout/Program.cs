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
// See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#consumer-timeouts
//
// This sample declares a quorum queue with a queue-level timeout, attaches a consumer with its own consumer-level
// timeout (highest precedence when both are set), and settles messages immediately so the timeout is not hit.

using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using IConnection = RabbitMQ.AMQP.Client.IConnection;
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

IConnection connection = await environment
    .CreateConnectionAsync(ConnectionSettingsBuilder.Create().Build())
    .ConfigureAwait(false);
Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

IManagement management = connection.Management();
const string queueName = "q_amqp10-consumer-timeout-example";

//TimeSpan queueConsumerTimeout = TimeSpan.FromSeconds(3);
TimeSpan attachConsumerTimeout = TimeSpan.FromSeconds(3);

IQueueSpecification queueSpec = management.Queue(queueName)
    .Quorum()
    // .ConsumerTimeout(queueConsumerTimeout)
    .Queue();

await queueSpec.DeclareAsync();

IPublisher publisher = await connection.PublisherBuilder().Queue(queueSpec).BuildAsync();

int secondsToWait = 4;
IConsumer consumer = await connection.ConsumerBuilder()
    .Queue(queueSpec)
    .Quorum()
    // There are different ways to configure the consumer timeout, 
    // see https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#consumer-timeouts
    
    .ConsumerTimeout(attachConsumerTimeout)
    .OnDeliveryRelease((context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information,
            $"[Consumer] Message: {message.BodyAsString()} released by consumer. Going to unlock the consumer");

        // Here we unlock the consumer from the consumer timeout state.
        // In this example, only one time has to raise the timeout, then we reset the secondsToWait to 0 to avoid
        // hitting the timeout for subsequent messages.
        context.Accept();
        return Task.CompletedTask;
    }).Builder()
    .MessageHandler(async (context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information,
            $"[Consumer] Message: {message.BodyAsString()} received; going to wait for {secondsToWait} seconds before accepting the message to trigger the consumer timeout");
        await Task.Delay(TimeSpan.FromSeconds(secondsToWait));
        secondsToWait =
            0; // only delay the first message to trigger the timeout, then reset to 0 for subsequent messages

        // In the first iteration the context.Accept() is ignored since id requeued due to timeout,
        // but in the second iteration it works as expected since the timeout is not hit.
        try
        {
            context.Accept();
            Trace.WriteLine(TraceLevel.Information, $"[Consumer] Message: {message.BodyAsString()} accepted");
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Error, $"[Consumer] Failed to accept message: {e.Message}");
        }
    })
    .BuildAndStartAsync();

Trace.WriteLine(TraceLevel.Information,
    $"Consumer attached with rabbitmq:consumer-timeout={(uint)attachConsumerTimeout.TotalMilliseconds} ms (per-consumer).");

const int total = 1;
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
