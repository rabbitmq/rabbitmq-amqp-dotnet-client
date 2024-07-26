﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
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
using EasyNetQ.Management.Client;
using EasyNetQ.Management.Client.Model;
using Xunit.Sdk;

namespace Tests
{
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
            var delaySpan = TimeSpan.FromMilliseconds(500);

            await Task.Delay(delaySpan);

            while (!await func())
            {
                await Task.Delay(delaySpan);

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
            public string? name { get; set; }
            public Dictionary<string, string>? client_properties { get; set; }
        }

        public static async Task<int> ConnectionsCountByName(string connectionName)
        {
            using HttpClientHandler handler = new HttpClientHandler();
            handler.Credentials = new NetworkCredential("guest", "guest");
            using HttpClient client = new HttpClient(handler);

            HttpResponseMessage result = await client.GetAsync("http://localhost:15672/api/connections");
            if (!result.IsSuccessStatusCode)
            {
                throw new XunitException(string.Format("HTTP GET failed: {0} {1}", result.StatusCode,
                    result.ReasonPhrase));
            }

            object? obj = await JsonSerializer.DeserializeAsync(await result.Content.ReadAsStreamAsync(),
                typeof(IEnumerable<Connection>));
            return obj switch
            {
                null => 0,
                IEnumerable<Connection> connections => connections.Sum(connection =>
                {
                    if (connection is null)
                    {
                        return 0;
                    }
                    else
                    {
                        if (connection.client_properties is not null &&
                            (connection.client_properties["connection_name"] == connectionName))
                        {
                            return 1;
                        }
                        else
                        {
                            return 0;
                        }
                    }
                }),
                _ => 0
            };
        }

        public static async Task<bool> IsConnectionOpen(string connectionName)
        {
            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            IReadOnlyList<EasyNetQ.Management.Client.Model.Connection> connections = await managementClient.GetConnectionsAsync();

            return connections.Any(conn =>
                    {
                        if (conn.ClientProperties is not null)
                        {
                            if (conn.ClientProperties.TryGetValue("connection_name", out object? connectionNameObj))
                            {
                                if (connectionNameObj is not null)
                                {
                                    string connName = (string)connectionNameObj;
                                    return connName.Contains(connectionName);
                                }
                            }
                        }

                        return false;
                    });
        }

        public static async Task<int> KillConnectionAsync(string connectionName)
        {
            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            IReadOnlyList<EasyNetQ.Management.Client.Model.Connection> connections = await managementClient.GetConnectionsAsync();

            // we kill _only_ producer and consumer connections
            // leave the locator up and running to delete the stream
            IEnumerable<EasyNetQ.Management.Client.Model.Connection> filteredConnections = connections.Where(conn =>
            {
                if (conn.ClientProperties is not null)
                {
                    if (conn.ClientProperties.TryGetValue("connection_name", out object? connectionNameObj))
                    {
                        if (connectionNameObj is not null)
                        {
                            string connName = (string)connectionNameObj;
                            return connName.Contains(connectionName);
                        }
                    }
                }

                return false;
            });

            int killed = 0;
            foreach (EasyNetQ.Management.Client.Model.Connection conn in filteredConnections)
            {
                try
                {
                    await managementClient.CloseConnectionAsync(conn);
                }
                catch (UnexpectedHttpStatusCodeException ex)
                {
                    if (ex.StatusCode != HttpStatusCode.NotFound)
                    {
                        throw;
                    }
                }

                killed += 1;
            }

            return killed;
        }

        public static async Task WaitUntilConnectionIsKilled(string connectionName)
        {
            await WaitUntilAsync(async () => await IsConnectionOpen(connectionName));
            Wait();
            await WaitUntilAsync(async () => await KillConnectionAsync(connectionName) == 1);
        }

        public static async Task WaitUntilConnectionIsKilledAndOpen(string connectionName)
        {
            await WaitUntilAsync(async () => await IsConnectionOpen(connectionName));
            Wait();
            await WaitUntilAsync(async () => await KillConnectionAsync(connectionName) == 1);
            Wait();
            await WaitUntilAsync(async () => await IsConnectionOpen(connectionName));
            Wait();
        }

        private static HttpClient CreateHttpClient()
        {
            var handler = new HttpClientHandler { Credentials = new NetworkCredential("guest", "guest"), };
            return new HttpClient(handler);
        }

        public static Task WaitUntilQueueExistsAsync(string queueNameStr)
        {
            return WaitUntilAsync(() =>
            {
                return CheckQueueAsync(queueNameStr, checkExisting: true);
            });
        }

        public static Task WaitUntilQueueDeletedAsync(string queueNameStr)
        {
            return WaitUntilAsync(() =>
            {
                return CheckQueueAsync(queueNameStr, checkExisting: false);
            });
        }

        public static Task WaitUntilExchangeExistsAsync(string exchangeNameStr)
        {
            return WaitUntilAsync(() =>
            {
                return CheckExchangeAsync(exchangeNameStr, checkExisting: true);
            });
        }

        public static Task WaitUntilExchangeDeletedAsync(string exchangeNameStr)
        {
            return WaitUntilAsync(() =>
            {
                return CheckExchangeAsync(exchangeNameStr, checkExisting: false);
            });
        }

        public static Task WaitUntilBindingsBetweenExchangeAndQueueExistAsync(string exchangeNameStr, string queueNameStr)
        {
            return WaitUntilAsync(() =>
            {
                return CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr, checkExisting: true);
            });
        }

        public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(string exchangeNameStr, string queueNameStr)
        {
            return WaitUntilAsync(() =>
            {
                return CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr, checkExisting: false);
            });
        }

        public static bool ArgsBindsBetweenExchangeAndQueueExists(string exchange, string queue,
            Dictionary<string, object> argumentsIn)
        {
            Task<HttpResponseMessage> resp = CreateHttpClient()
                .GetAsync(
                    $"http://localhost:15672/api/bindings/%2F/e/{Uri.EscapeDataString(exchange)}/q/{Uri.EscapeDataString(queue)}");
            resp.Wait(TimeSpan.FromSeconds(10));
            string body = resp.Result.Content.ReadAsStringAsync().Result;
            if (body == "[]")
            {
                return false;
            }

            List<Dictionary<string, object>>? bindingsResults = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(body);
            if (bindingsResults is not null)
            {
                foreach (Dictionary<string, object> argumentResult in bindingsResults)
                {
                    Dictionary<string, object>? argumentsResult = JsonSerializer.Deserialize<Dictionary<string, object>>(
                        argumentResult["arguments"].ToString() ??
                        throw new InvalidOperationException());
                    // Check only the key to avoid conversion value problems 
                    // on the test is enough to avoid to put the same key
                    // at some point we could add keyValuePair.Value == keyValuePairResult.Value
                    // keyValuePairResult.Value is a json object
                    if (argumentsResult is not null)
                    {
                        IEnumerable<string> results = argumentsResult.Keys.ToArray()
                            .Intersect(argumentsIn.Keys.ToArray(), StringComparer.OrdinalIgnoreCase);
                        if (results.Count() == argumentsIn.Count)
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        public static bool BindsBetweenExchangeAndExchangeExists(string sourceExchange, string destinationExchange)
        {
            Task<HttpResponseMessage> resp = CreateHttpClient()
                .GetAsync(
                    $"http://localhost:15672/api/bindings/%2F/e/{Uri.EscapeDataString(sourceExchange)}/e/{Uri.EscapeDataString(destinationExchange)}");
            resp.Wait(TimeSpan.FromSeconds(10));
            string body = resp.Result.Content.ReadAsStringAsync().Result;
            return body != "[]" && resp.Result.IsSuccessStatusCode;
        }


        public static int HttpGetQMsgCount(string queue)
        {
            var task = CreateHttpClient()
                .GetAsync($"http://localhost:15672/api/queues/%2F/{Uri.EscapeDataString(queue)}");
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
            Task<HttpResponseMessage> task = CreateHttpClient().DeleteAsync($"http://localhost:15672/api/queues/%2F/{queue}");
            task.Wait();
            HttpResponseMessage result = task.Result;
            if (!result.IsSuccessStatusCode && result.StatusCode != HttpStatusCode.NotFound)
            {
                throw new XunitException($"HTTP DELETE failed: {result.StatusCode} {result.ReasonPhrase}");
            }
        }

        public static byte[] GetFileContent(string fileName)
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().Location);
            string codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            string? dirPath = Path.GetDirectoryName(codeBasePath);
            if (dirPath is null)
            {
                return [];
            }

            string filename = Path.Combine(dirPath, "Resources", fileName);
            Task<byte[]> fileTask = File.ReadAllBytesAsync(filename);
            fileTask.Wait(TimeSpan.FromSeconds(1));
            return fileTask.Result;
        }

        private static async Task<bool> CheckExchangeAsync(string exchangeNameStr, bool checkExisting = true)
        {
            // Assume success
            bool rv = true;

            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            var exchangeName = new ExchangeName(exchangeNameStr, "/");
            try
            {
                Exchange? exchange = await managementClient.GetExchangeAsync(exchangeName);
                if (checkExisting)
                {
                    rv = exchange is not null;
                }
                else
                {
                    rv = exchange is null;
                }
            }
            catch (UnexpectedHttpStatusCodeException ex)
            {
                if (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    if (checkExisting)
                    {
                        rv = false;
                    }
                    else
                    {
                        rv = true;
                    }
                }
                else
                {
                    throw;
                }
            }

            return rv;
        }

        private static async Task<bool> CheckQueueAsync(string queueNameStr, bool checkExisting = true)
        {
            // Assume success
            bool rv = true;

            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            var queueName = new QueueName(queueNameStr, "/");
            try
            {
                Queue? queue = await managementClient.GetQueueAsync(queueName);
                if (checkExisting)
                {
                    rv = queue is not null;
                }
                else
                {
                    rv = queue is null;
                }
            }
            catch (UnexpectedHttpStatusCodeException ex)
            {
                if (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    if (checkExisting)
                    {
                        rv = false;
                    }
                    else
                    {
                        rv = true;
                    }
                }
                else
                {
                    throw;
                }
            }

            return rv;
        }

        private static async Task<bool> CheckBindingsBetweenExchangeAndQueueAsync(string exchangeNameStr, string queueNameStr, bool checkExisting = true)
        {
            // Assume success
            bool rv = true;

            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            var exchangeName = new ExchangeName(exchangeNameStr, "/");
            var queueName = new QueueName(queueNameStr, "/");
            try
            {
                IReadOnlyList<Binding> bindings = await managementClient.GetQueueBindingsAsync(exchangeName, queueName);
                if (checkExisting)
                {
                    if (bindings.Count == 0)
                    {
                        rv = false;
                    }
                }
                else
                {
                    if (bindings.Count > 0)
                    {
                        rv = false;
                    }
                }
            }
            catch (UnexpectedHttpStatusCodeException ex)
            {
                if (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    if (checkExisting)
                    {
                        rv = false;
                    }
                }
                else
                {
                    throw;
                }
            }

            return rv;
        }
    }
}
