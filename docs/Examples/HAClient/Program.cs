﻿using System.Diagnostics;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;
ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

long messagesReceived = 0;
long messagesConfirmed = 0;
long notMessagesConfirmed = 0;
long messagesFailed = 0;

const int totalMessagesToSend = 5_000_000;


Task printStats = Task.Run(() =>
{
    while (true)
    {
        Trace.WriteLine(TraceLevel.Information, (
            $"[(Confirmed: {Interlocked.Read(ref messagesConfirmed)}, " +
            $"Failed: {Interlocked.Read(ref messagesFailed)}, UnConfirmed: {Interlocked.Read(ref notMessagesConfirmed)} )] " +
            $"[(Received: {Interlocked.Read(ref messagesReceived)})] " +
            $"(Un/Confirmed+Failed : {messagesConfirmed + messagesFailed + notMessagesConfirmed} ) "));
        Thread.Sleep(1000);
    }
});


Trace.WriteLine(TraceLevel.Information, "Starting");
const string connectionName = "HA-Client-Connection";

IEnvironment environment = await AmqpEnvironment.CreateAsync(ConnectionSettingBuilder.Create().ConnectionName(connectionName).Build()).ConfigureAwait(false);

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

connection.ChangeState += (sender, fromState, toState, e) =>
{
    Trace.WriteLine(TraceLevel.Information, $"Connection State Changed from {fromState} to {toState}");
};

Trace.WriteLine(TraceLevel.Information, "Connected");

IManagement management = connection.Management();
const string queueName = "ha-amqp10-client-test";
await management.QueueDeletion().Delete(queueName).ConfigureAwait(false);

await management.Queue(queueName).Type(QueueType.QUORUM).Declare().ConfigureAwait(false);
IPublisher publisher = connection.PublisherBuilder().Queue(queueName).MaxInflightMessages(2000).Build();

ManualResetEvent pausePublishing = new(true);
publisher.ChangeState += (sender, fromState, toState, e) =>
{
    Trace.WriteLine(TraceLevel.Information, $"Publisher State Changed, from {fromState} to {toState}");

    if (toState == State.Open)
    {
        pausePublishing.Set();
    }
    else
    {
        pausePublishing.Reset();
    }
};


IConsumer consumer = connection.ConsumerBuilder().Queue(queueName).InitialCredits(100).MessageHandler(
    (context, message) =>
    {
        Interlocked.Increment(ref messagesReceived);
        context.Accept();
    }
).Build();


consumer.ChangeState += (sender, fromState, toState, e) =>
{
    Trace.WriteLine(TraceLevel.Information, $"Consumer State Changed, from {fromState} to {toState}");
};


for (int i = 0; i < totalMessagesToSend; i++)
{
    try
    {
        pausePublishing.WaitOne();
        await publisher.Publish(
            new AmqpMessage($"Hello World_{i}"),
            (message, descriptor) =>
            {
                if (descriptor.State == OutcomeState.Accepted)
                {
                    Interlocked.Increment(ref messagesConfirmed);
                }
                else
                {
                    Interlocked.Increment(ref notMessagesConfirmed);
                }
            }).ConfigureAwait(false);
    }
    catch (Exception e)
    {
        Trace.WriteLine(TraceLevel.Error, $"Failed to publish message, {e.Message}");
        Interlocked.Increment(ref messagesFailed);
        await Task.Delay(500).ConfigureAwait(false);
    }
}


Trace.WriteLine(TraceLevel.Information, "Queue Created");
Console.WriteLine("Press any key to delete the queue and close the connection.");
Console.ReadKey();
await publisher.CloseAsync().ConfigureAwait(false);
await consumer.CloseAsync().ConfigureAwait(false);
await management.QueueDeletion().Delete(queueName).ConfigureAwait(false);
await connection.CloseAsync().ConfigureAwait(false);
printStats.Dispose();
Trace.WriteLine(TraceLevel.Information, "Closed");
