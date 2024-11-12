// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
const string containerId = "getting-started-Connection";

IEnvironment environment = AmqpEnvironment.Create(
    ConnectionSettingBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync();

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");

// ------------------------------------------------------------------------------------
// The management object is used to declare/delete queues, exchanges, and bindings
IManagement management = connection.Management();
const string exchangeName = "e_amqp10-client-test";
const string queueName = "q_amqp10-client-test";
const string routingKey = "routing_key";
IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.QUORUM);
await queueSpec.DeclareAsync();
IExchangeSpecification exchangeSpec = management.Exchange(exchangeName).Type(ExchangeType.TOPIC);
await exchangeSpec.DeclareAsync();

IBindingSpecification bindingSpec = management.Binding()
    .SourceExchange(exchangeSpec)
    .DestinationQueue(queueSpec)
    .Key(routingKey);
await bindingSpec.BindAsync();
Trace.WriteLine(TraceLevel.Information,
    $"Queue {queueName} and Exchange {exchangeName} declared and bound with key {routingKey} successfully");
// ------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------
// Declare a publisher and a consumer.
// The publisher can use exchange (optionally with a key) or queue to publish messages. 
IPublisher publisher = await connection.PublisherBuilder().Exchange(exchangeName).Key(routingKey)
    .BuildAsync();

IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).MessageHandler(
    (context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information, $"[Consumer] Message: {message.Body()} received");
        context.Accept();
        return Task.CompletedTask;
    }
).BuildAndStartAsync();
// ------------------------------------------------------------------------------------

const int total = 10;
for (int i = 0; i < total; i++)
{
    var message = new AmqpMessage($"Hello World_{i}");
    PublishResult pr = await publisher.PublishAsync(message);
    switch (pr.Outcome.State)
    {
        case OutcomeState.Accepted:
            Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message: {message.Body()} confirmed");
            break;
        case OutcomeState.Released:
            Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message: {message.Body()} Released");
            break;

        case OutcomeState.Rejected:
            Trace.WriteLine(TraceLevel.Error,
                $"[Publisher] Message: {message.Body()} Rejected with error: {pr.Outcome.Error}");
            break;
        default:
            throw new ArgumentOutOfRangeException();
    }
}

Console.WriteLine("Press any key to delete queue, exchange and close the environment.");
Console.ReadKey();

await publisher.CloseAsync();
publisher.Dispose();

await consumer.CloseAsync();
consumer.Dispose();

await queueSpec.DeleteAsync();
await exchangeSpec.DeleteAsync();

await environment.CloseAsync();

Trace.WriteLine(TraceLevel.Information, "Example closed successfully");
