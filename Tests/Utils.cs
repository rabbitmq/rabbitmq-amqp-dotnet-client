// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests
{
    public class Utils<TResult>(ITestOutputHelper testOutputHelper)
    {
        public void WaitUntilTaskCompletes(TaskCompletionSource<TResult> tasks)
        {
            WaitUntilTaskCompletes(tasks, true, TimeSpan.FromSeconds(10));
        }

        public void WaitUntilTaskCompletes(TaskCompletionSource<TResult> tasks,
            bool expectToComplete = true)
        {
            WaitUntilTaskCompletes(tasks, expectToComplete, TimeSpan.FromSeconds(10));
        }

        public void WaitUntilTaskCompletes(TaskCompletionSource<TResult> tasks,
            bool expectToComplete,
            TimeSpan timeOut)
        {
            try
            {
                var resultTestWait = tasks.Task.Wait(timeOut);
                Assert.Equal(resultTestWait, expectToComplete);
            }
            catch (Exception e)
            {
                testOutputHelper.WriteLine($"wait until task completes error #{e}");
                throw;
            }
        }
    }

    public static class SystemUtils
    {
        // Waits for 10 seconds total by default
        public static void WaitUntil(Func<bool> func, ushort retries = 40)
        {
            while (!func())
            {
                Wait(TimeSpan.FromMilliseconds(250));
                --retries;
                if (retries == 0)
                {
                    throw new XunitException("timed out waiting on a condition!");
                }
            }
        }

        public static async Task WaitUntilAsync(Func<Task<bool>> func, ushort retries = 10)
        {
            Wait();
            while (!await func())
            {
                Wait();
                --retries;
                if (retries == 0)
                {
                    throw new XunitException("timed out waiting on a condition!");
                }
            }
        }

        public static void Wait()
        {
            Thread.Sleep(TimeSpan.FromMilliseconds(500));
        }

        public static void Wait(TimeSpan wait)
        {
            Thread.Sleep(wait);
        }


        private class Connection
        {
            public string name { get; set; }
            public Dictionary<string, string> client_properties { get; set; }
        }

        public static async Task<int> ConnectionsCountByName(string connectionName)
        {
            using var handler = new HttpClientHandler();
            handler.Credentials = new NetworkCredential("guest", "guest");
            using var client = new HttpClient(handler);

            var result = await client.GetAsync("http://localhost:15672/api/connections");
            if (!result.IsSuccessStatusCode)
            {
                throw new XunitException(string.Format("HTTP GET failed: {0} {1}", result.StatusCode,
                    result.ReasonPhrase));
            }

            var obj = await JsonSerializer.DeserializeAsync(await result.Content.ReadAsStreamAsync(),
                typeof(IEnumerable<Connection>));
            return obj switch
            {
                null => 0,
                IEnumerable<Connection> connections => connections.Sum(connection =>
                    connection.client_properties["connection_name"] == connectionName ? 1 : 0),
                _ => 0
            };
        }

        public static async Task<bool> IsConnectionOpen(string connectionName)
        {
            using var handler = new HttpClientHandler { Credentials = new NetworkCredential("guest", "guest"), };
            using var client = new HttpClient(handler);
            bool isOpen = false;

            var result = await client.GetAsync("http://localhost:15672/api/connections");
            if (!result.IsSuccessStatusCode)
            {
                throw new XunitException($"HTTP GET failed: {result.StatusCode} {result.ReasonPhrase}");
            }

            object obj = await JsonSerializer.DeserializeAsync(await result.Content.ReadAsStreamAsync(),
                typeof(IEnumerable<Connection>));
            switch (obj)
            {
                case null:
                    return false;
                case IEnumerable<Connection> connections:
                    isOpen = connections.Any(x => x.client_properties["connection_name"].Contains(connectionName));
                    break;
            }

            return isOpen;
        }

        public static async Task<int> HttpKillConnections(string connectionName)
        {
            using var handler = new HttpClientHandler();
            handler.Credentials = new NetworkCredential("guest", "guest");
            using var client = new HttpClient(handler);

            var result = await client.GetAsync("http://localhost:15672/api/connections");
            if (!result.IsSuccessStatusCode && result.StatusCode != HttpStatusCode.NotFound)
            {
                throw new XunitException($"HTTP GET failed: {result.StatusCode} {result.ReasonPhrase}");
            }

            var json = await result.Content.ReadAsStringAsync();
            var connections = JsonSerializer.Deserialize<IEnumerable<Connection>>(json);
            if (connections == null)
            {
                return 0;
            }

            // we kill _only_ producer and consumer connections
            // leave the locator up and running to delete the stream
            var iEnumerable = connections.Where(x => x.client_properties["connection_name"].Contains(connectionName));
            var enumerable = iEnumerable as Connection[] ?? iEnumerable.ToArray();
            int killed = 0;
            foreach (var conn in enumerable)
            {
                /*
                 * NOTE:
                 * this is the equivalent to this JS code:
                 * https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_management/priv/www/js/formatters.js#L710-L712
                 *
                 * function esc(str) {
                 *   return encodeURIComponent(str);
                 * }
                 *
                 * https://stackoverflow.com/a/4550600
                 */
                var s = Uri.EscapeDataString(conn.name);
                var deleteResult = await client.DeleteAsync($"http://localhost:15672/api/connections/{s}");
                if (!deleteResult.IsSuccessStatusCode && result.StatusCode != HttpStatusCode.NotFound)
                {
                    throw new XunitException(
                        $"HTTP DELETE failed: {deleteResult.StatusCode} {deleteResult.ReasonPhrase}");
                }

                killed += 1;
            }

            return killed;
        }

        public static async Task WaitUntilConnectionIsKilled(string connectionName)
        {
            await WaitUntilAsync(async () => await IsConnectionOpen(connectionName));
            Wait();
            await WaitUntilAsync(async () => await HttpKillConnections(connectionName) == 1);
        }

        private static HttpClient CreateHttpClient()
        {
            var handler = new HttpClientHandler { Credentials = new NetworkCredential("guest", "guest"), };
            return new HttpClient(handler);
        }



        public static bool QueueExists(string queue)
        {
            var task = CreateHttpClient().GetAsync($"http://localhost:15672/api/queues/%2F/{queue}");
            task.Wait(TimeSpan.FromSeconds(10));
            var result = task.Result;
            return result.IsSuccessStatusCode;
        }

        public static bool ExchangeExists(string exchange)
        {
            var task = CreateHttpClient().GetAsync($"http://localhost:15672/api/exchanges/%2F/{exchange}");
            task.Wait(TimeSpan.FromSeconds(10));
            var result = task.Result;
            return result.IsSuccessStatusCode;
        }

        public static bool BindsBetweenExchangeAndQueueExists(string exchange, string queue)
        {
            var resp = CreateHttpClient().GetAsync($"http://localhost:15672/api/bindings/%2F/e/{exchange}/q/{queue}");
            resp.Wait(TimeSpan.FromSeconds(10));
            string body = resp.Result.Content.ReadAsStringAsync().Result;
            return body != "[]" && resp.Result.IsSuccessStatusCode;
        }

        public static bool BindsBetweenExchangeAndExchangeExists(string sourceExchange, string destinationExchange)
        {
            var resp = CreateHttpClient().GetAsync($"http://localhost:15672/api/bindings/%2F/e/{sourceExchange}/e/{destinationExchange}");
            resp.Wait(TimeSpan.FromSeconds(10));
            string body = resp.Result.Content.ReadAsStringAsync().Result;
            return body != "[]" && resp.Result.IsSuccessStatusCode;
        }



        public static int HttpGetQMsgCount(string queue)
        {
            var task = CreateHttpClient().GetAsync($"http://localhost:15672/api/queues/%2F/{queue}");
            task.Wait(TimeSpan.FromSeconds(10));
            var result = task.Result;
            if (!result.IsSuccessStatusCode)
            {
                throw new XunitException($"HTTP GET failed: {result.StatusCode} {result.ReasonPhrase}");
            }

            var responseBody = result.Content.ReadAsStringAsync();
            responseBody.Wait(TimeSpan.FromSeconds(10));
            string json = responseBody.Result;
            var obj = JsonSerializer.Deserialize<Dictionary<string, object>>(json);
            if (obj == null)
            {
                return 0;
            }

            return obj.TryGetValue("messages_ready", out var value) ? Convert.ToInt32(value.ToString()) : 0;
        }

        public static void HttpPost(string jsonBody, string api)
        {
            HttpContent content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            var task = CreateHttpClient().PostAsync($"http://localhost:15672/api/{api}", content);
            task.Wait();
            var result = task.Result;
            if (!result.IsSuccessStatusCode)
            {
                throw new XunitException(string.Format("HTTP POST failed: {0} {1}", result.StatusCode,
                    result.ReasonPhrase));
            }
        }

        public static void HttpDeleteQueue(string queue)
        {
            var task = CreateHttpClient().DeleteAsync($"http://localhost:15672/api/queues/%2F/{queue}");
            task.Wait();
            var result = task.Result;
            if (!result.IsSuccessStatusCode && result.StatusCode != HttpStatusCode.NotFound)
            {
                throw new XunitException($"HTTP DELETE failed: {result.StatusCode} {result.ReasonPhrase}");
            }
        }

        public static byte[] GetFileContent(string fileName)
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().Location);
            string codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            string dirPath = Path.GetDirectoryName(codeBasePath);
            if (dirPath == null)
            {
                return null;
            }

            string filename = Path.Combine(dirPath, "Resources", fileName);
            var fileTask = File.ReadAllBytesAsync(filename);
            fileTask.Wait(TimeSpan.FromSeconds(1));
            return fileTask.Result;
        }
    }
}
