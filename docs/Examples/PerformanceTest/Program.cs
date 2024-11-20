// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Diagnostics;
using PerformanceTest;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

// ---- Configuration ----
const int total = 5_000_000;
const int tasksSize = 200;
bool enableConsumer = true;
// -----------------------

Trace.TraceLevel = TraceLevel.Verbose;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

Trace.WriteLine(TraceLevel.Information, "Starting performance test...");
const string containerId = "performance-test-connection";

IEnvironment environment = AmqpEnvironment.Create(ConnectionSettingBuilder.Create().ContainerId(containerId).Build());

IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, "Connected");

IManagement management = connection.Management();

const string queueName = "amqp10-net-perf-test";
IQueueSpecification queueSpec = management.Queue(queueName).Type(QueueType.QUORUM);
await queueSpec.DeleteAsync();
await queueSpec.DeclareAsync();
Trace.WriteLine(TraceLevel.Information, $"Queue {queueName} recreated");
Stats stats = new();
IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

Task MessageHandler(IContext context, IMessage message)
{
    context.Accept();
    stats.IncrementConsumed();
    return Task.CompletedTask;
}

IConsumer? consumer = null;

if (enableConsumer)
{
    consumer = await connection.ConsumerBuilder()
        .Queue(queueName)
        .InitialCredits(1000)
        .MessageHandler(MessageHandler)
        .BuildAndStartAsync();
}

stats.Start();
_ = Task.Run(async () =>
{
    while (stats.IsRunning())
    {
        await Task.Delay(1000);
        Trace.WriteLine(TraceLevel.Information, $"{stats.Report()}");
    }
});

List<Task<PublishResult>> tasks = [];

try
{
    for (int i = 0; i < (total / tasksSize); i++)
    {
        try
        {
            for (int j = 0; j < tasksSize; j++)
            {
                var message = new AmqpMessage(new byte[10]);
                tasks.Add(publisher.PublishAsync(message));
                stats.IncrementPublished();
            }

            foreach (var result in await Task.WhenAll(tasks))
            {
                if (result.Outcome.State == OutcomeState.Accepted)
                {
                    stats.IncrementAccepted();
                }
                else
                {
                    stats.IncrementFailed();
                }
            }

            tasks.Clear();
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Error, $"{e.Message}");
        }
    }

    stats.Stop();
    await Task.Delay(1000);
    Trace.WriteLine(TraceLevel.Information, $"Consumer: {enableConsumer} - TaskSize: {tasksSize} - {stats.Report(true)}");
}
catch (Exception e)
{
    Trace.WriteLine(TraceLevel.Error, $"{e.Message}");
}
finally
{
    Console.WriteLine("Press any key to delete the queue and close the connection.");
    Console.ReadKey();

    try
    {
        await publisher.CloseAsync();
        publisher.Dispose();
    }
    catch (Exception ex)
    {
        Console.WriteLine("[ERROR] unexpected exception while closing publisher: {0}", ex);
    }

    try
    {
        if (consumer != null)
        {
            await consumer.CloseAsync();
            consumer.Dispose();
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine("[ERROR] unexpected exception while closing consumer: {0}", ex);
    }

    try
    {
        await queueSpec.DeleteAsync();
    }
    catch (Exception ex)
    {
        Console.WriteLine("[ERROR] unexpected exception while deleting queue: {0}", ex);
    }

    try
    {
        await environment.CloseAsync();
    }
    catch (Exception ex)
    {
        Console.WriteLine("[ERROR] unexpected exception while closing environment: {0}", ex);
    }
}
