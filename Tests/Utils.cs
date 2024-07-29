// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using EasyNetQ.Management.Client;
using EasyNetQ.Management.Client.Model;
using Xunit.Sdk;

namespace Tests
{
    public static class SystemUtils
    {
        private static readonly TimeSpan s_initialDelaySpan = TimeSpan.FromMilliseconds(100);
        private static readonly TimeSpan s_shortDelaySpan = TimeSpan.FromMilliseconds(250);
        private static readonly TimeSpan s_delaySpan = TimeSpan.FromMilliseconds(500);

        public static async Task WaitUntilFuncAsync(Func<bool> func, ushort retries = 40)
        {
            await Task.Delay(s_initialDelaySpan);

            while (false == func())
            {
                await Task.Delay(s_shortDelaySpan);

                --retries;
                if (retries == 0)
                {
                    throw new XunitException("timed out waiting on a condition!");
                }
            }
        }

        public static async Task WaitUntilAsync(Func<Task<bool>> func, ushort retries = 10)
        {
            await Task.Delay(s_initialDelaySpan);

            while (false == await func())
            {
                await Task.Delay(s_delaySpan);

                --retries;
                if (retries == 0)
                {
                    throw new XunitException("timed out waiting on a condition!");
                }
            }
        }

        public static Task WaitUntilConnectionIsOpen(string connectionName)
        {
            return WaitUntilAsync(() => CheckConnectionAsync(connectionName, checkOpened: true));
        }

        public static Task WaitUntilConnectionIsClosed(string connectionName)
        {
            return WaitUntilAsync(() => CheckConnectionAsync(connectionName, checkOpened: false));
        }

        public static async Task WaitUntilConnectionIsKilled(string connectionName)
        {
            await WaitUntilConnectionIsOpen(connectionName);
            await WaitUntilAsync(async () => await KillConnectionAsync(connectionName) == 1);
        }

        public static async Task WaitUntilConnectionIsKilledAndOpen(string connectionName)
        {
            await WaitUntilConnectionIsOpen(connectionName);
            await WaitUntilAsync(async () => await KillConnectionAsync(connectionName) == 1);
            await WaitUntilConnectionIsOpen(connectionName);
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

        public static Task WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(string exchangeNameStr, string queueNameStr,
            Dictionary<string, object> args)
        {
            return WaitUntilAsync(() =>
            {
                return CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                    args: args, checkExisting: true);
            });
        }

        public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(string exchangeNameStr, string queueNameStr,
            Dictionary<string, object> args)
        {
            return WaitUntilAsync(() =>
            {
                return CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                    args: args, checkExisting: false);
            });
        }

        public static Task WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(string sourceExchangeNameStr, string destinationExchangeNameStr)
        {
            return WaitUntilAsync(() =>
            {
                return CheckBindingsBetweenExchangeAndExchangeAsync(sourceExchangeNameStr, destinationExchangeNameStr, checkExisting: true);
            });
        }

        public static Task WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(string sourceExchangeNameStr, string destinationExchangeNameStr)
        {
            return WaitUntilAsync(() =>
            {
                return CheckBindingsBetweenExchangeAndExchangeAsync(sourceExchangeNameStr, destinationExchangeNameStr, checkExisting: false);
            });
        }

        public static Task WaitUntilQueueMessageCount(string queueNameStr, long messageCount)
        {
            return WaitUntilAsync(async () =>
            {
                long queueMessageCount = await GetQueueMessageCountAsync(queueNameStr);
                return messageCount == queueMessageCount;
            }, retries: 20);
        }

        private static async Task<bool> CheckConnectionAsync(string connectionName, bool checkOpened = true)
        {
            bool rv = true;

            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            IReadOnlyList<Connection> connections = await managementClient.GetConnectionsAsync();
            rv = connections.Any(conn =>
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

            if (false == checkOpened)
            {
                return !rv;
            }
            else
            {
                return rv;
            }
        }

        private static async Task<int> KillConnectionAsync(string connectionName)
        {
            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            IReadOnlyList<Connection> connections = await managementClient.GetConnectionsAsync();

            // we kill _only_ producer and consumer connections
            // leave the locator up and running to delete the stream
            IEnumerable<Connection> filteredConnections = connections.Where(conn =>
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
            foreach (Connection conn in filteredConnections)
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

        private static async Task<long> GetQueueMessageCountAsync(string queueNameStr)
        {
            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            var queueName = new QueueName(queueNameStr, "/");
            Queue queue = await managementClient.GetQueueAsync(queueName);
            return queue.MessagesReady;
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

        private static async Task<bool> CheckBindingsBetweenExchangeAndQueueAsync(string exchangeNameStr, string queueNameStr,
            Dictionary<string, object>? args = null, bool checkExisting = true)
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
                    if (args is not null)
                    {
                        // We're checking that arguments are equivalent, too
                        foreach (Binding b in bindings)
                        {
                            if (b.Arguments is null)
                            {
                                rv = false;
                                break;
                            }

                            // Check only the key to avoid conversion value problems 
                            // on the test is enough to avoid to put the same key
                            // at some point we could add keyValuePair.Value == keyValuePairResult.Value
                            // keyValuePairResult.Value is a json object
                            IEnumerable<string> results = b.Arguments.Keys.Intersect(args.Keys, StringComparer.OrdinalIgnoreCase);
                            if (results.Count() == args.Count)
                            {
                                rv = true;
                                break;
                            }
                        }
                    }
                    else
                    {
                        if (bindings.Count == 0)
                        {
                            rv = false;
                        }
                    }
                }
                else
                {
                    if (args is not null)
                    {
                        bool foundMatchingBinding = false;
                        // We're checking that no bindings have the passed-in args
                        // So, if we go through all bindings and all args are different,
                        // we can assume the binding we're checking for is gone
                        foreach (Binding b in bindings)
                        {
                            if (b.Arguments is not null)
                            {
                                // Check only the key to avoid conversion value problems 
                                // on the test is enough to avoid to put the same key
                                // at some point we could add keyValuePair.Value == keyValuePairResult.Value
                                // keyValuePairResult.Value is a json object
                                IEnumerable<string> results = b.Arguments.Keys.Intersect(args.Keys, StringComparer.OrdinalIgnoreCase);
                                if (results.Count() == args.Count)
                                {
                                    foundMatchingBinding = true;
                                    break;
                                }
                            }
                        }

                        if (foundMatchingBinding)
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

        private static async Task<bool> CheckBindingsBetweenExchangeAndExchangeAsync(string sourceExchangeNameStr, string destinationExchangeNameStr,
            bool checkExisting = true)
        {
            // Assume success
            bool rv = true;

            var managementUri = new Uri("http://localhost:15672");
            using var managementClient = new ManagementClient(managementUri, "guest", "guest");

            var sourceExchangeName = new ExchangeName(sourceExchangeNameStr, "/");
            var destinationExchangeName = new ExchangeName(destinationExchangeNameStr, "/");
            try
            {
                IReadOnlyList<Binding> bindings = await managementClient.GetExchangeBindingsAsync(sourceExchangeName, destinationExchangeName);
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
