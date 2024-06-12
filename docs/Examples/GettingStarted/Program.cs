// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using RabbitMQ.AMQP.Client.Impl;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

Trace.TraceLevel = TraceLevel.Information;

ConsoleTraceListener consoleListener = new();
Trace.TraceListener = (l, f, a) =>
    consoleListener.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, a));

// static async Task SendRequestAsync(TaskCompletionSource<string> tcs)
// {
//     Trace.WriteLine(TraceLevel.Information, "Sending request...");
//         
//     // Simulate some delay for the request
//     await Task.Delay(2000);
//         
//     // Simulate setting the response
//     tcs.SetResult("Response received!");
//     
//     Trace.WriteLine(TraceLevel.Information, "Request sent and response set");
// }
//
// static async Task WaitForResponseAsync(TaskCompletionSource<string> tcs)
// {
//     Trace.WriteLine(TraceLevel.Information, "Waiting for response...");
//
//     
//         
//     // Wait for the response to be set
//     string response = await tcs.Task;
//     
//     Trace.WriteLine(TraceLevel.Information, "Response received");
//         
//     await Task.Delay(20000);
//     
//     Trace.WriteLine(TraceLevel.Information, "Response processed");
// }
//
// // Create a TaskCompletionSource to manage the response task
// var tcs = new TaskCompletionSource<string>();
//
// // Start the request task
// var requestTask = SendRequestAsync(tcs);
//
// // Start the response task
// var responseTask = WaitForResponseAsync(tcs);
//
// // Wait for both tasks to complete
// await Task.WhenAll(requestTask, responseTask);






Trace.WriteLine(TraceLevel.Information, "Starting");
AmqpConnection connection = new();

var connectionName = Guid.NewGuid().ToString();
await connection.ConnectAsync(new AmqpAddressBuilder().ConnectionName(connectionName).Build());
Trace.WriteLine(TraceLevel.Information, "Connected");
var management = connection.Management();
await management.Queue("re-recreate-queue").AutoDelete(true).Exclusive(true).Declare();
// await management.Queue("re1-recreate-queue").AutoDelete(true).Exclusive(true).Declare();


Console.WriteLine("Press any key to close connection");
Console.ReadKey();
await connection.CloseAsync();
Trace.WriteLine(TraceLevel.Information, "Closed");