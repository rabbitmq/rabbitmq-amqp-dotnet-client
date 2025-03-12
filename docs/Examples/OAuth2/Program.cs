// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using OAuth2;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Verbose;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine($"[{DateTime.Now}] [{l}] - {f}");

IConnection connection = await AmqpConnection.CreateAsync(
    ConnectionSettingsBuilder.Create()
        .Host("localhost")
        .Port(5672)
        .OAuth2Options(new OAuth2Options(Token.GenerateToken(DateTime.UtcNow.AddMilliseconds(1500))))
        .Build()).ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");
Trace.WriteLine(TraceLevel.Information, $"Connection status {connection.State}");

Thread.Sleep(TimeSpan.FromSeconds(15));

Console.WriteLine("Connection state: " + connection.State);
