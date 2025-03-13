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
        .OAuth2Options(new OAuth2Options(Token.GenerateToken(DateTime.UtcNow.AddSeconds(5))))
        .Build()).ConfigureAwait(false);

Trace.WriteLine(TraceLevel.Information, $"Connected to the broker {connection} successfully");
Trace.WriteLine(TraceLevel.Information, $"Connection status {connection.State}");

CancellationTokenSource cts = new();

_ = Task.Run(() =>
{
    while (!cts.IsCancellationRequested)
    {
        string token = Token.GenerateToken(DateTime.UtcNow.AddSeconds(5));
        Trace.WriteLine(TraceLevel.Information, $"Token Refresh..{token}");
        connection.RefreshTokenAsync(token).Wait();
        Task.Delay(TimeSpan.FromSeconds(4), cts.Token).Wait();
    }
});


Console.WriteLine("Connection state: " + connection.State);

// press any key to exit
Console.ReadKey();
cts.Cancel();

await connection.CloseAsync().ConfigureAwait(false);
