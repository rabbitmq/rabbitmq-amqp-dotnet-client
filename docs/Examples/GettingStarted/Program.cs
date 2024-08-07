﻿using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Starting");
const string connectionName = "GettingStarted-Connection";

IEnvironment environment = await AmqpEnvironment.CreateAsync(
    ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build());

IConnection connection = await environment.CreateConnectionAsync();

Trace.WriteLine(TraceLevel.Information, "Connected");

IManagement management = connection.Management();
const string queueName = "amqp10-client-test";
await management.QueueDeletion().Delete(queueName).ConfigureAwait(false);

await management.Queue(queueName).Type(QueueType.QUORUM).Declare();

IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).MaxInflightMessages(2000).BuildAsync();

IConsumer consumer = await connection.ConsumerBuilder().Queue(queueName).InitialCredits(100).MessageHandler(
    (context, message) =>
    {
        Trace.WriteLine(TraceLevel.Information, $"[Consumer] Message: {message.Body()} received");
        context.Accept();
    }
).BuildAsync();

const int total = 10;
for (int i = 0; i < total; i++)
{
    await publisher.Publish(
        new AmqpMessage($"Hello World_{i}"),
        (message, descriptor) =>
        {
            if (descriptor.State == OutcomeState.Accepted)
            {
                Trace.WriteLine(TraceLevel.Information, $"[Publisher] Message: {message.Body()} confirmed");
            }
            else
            {
                Trace.WriteLine(TraceLevel.Error,
                    $"outcome result, state: {descriptor.State}, code: {descriptor.Code}, message_id: " +
                    $"{message.MessageId()} Description: {descriptor.Description}, error: {descriptor.Error}");
            }
        });
}


Trace.WriteLine(TraceLevel.Information, "Queue Created");
Console.WriteLine("Press any key to delete the queue and close the connection.");
Console.ReadKey();
await publisher.CloseAsync();
await consumer.CloseAsync();
await management.QueueDeletion().Delete(queueName);
await environment.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Closed");
