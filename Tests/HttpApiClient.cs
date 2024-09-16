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

namespace Tests;

public class HttpApiClient : IDisposable
{
    private readonly ManagementClient _managementClient;

    public HttpApiClient()
    {
        _managementClient = SystemUtils.InitManagementClient();
    }

    public ushort GetClusterSize()
    {
        IReadOnlyList<Node> nodes = _managementClient.GetNodes();
        return (ushort)nodes.Count;
    }

    /// <summary>
    /// Creates a user, without password, with full permissions
    /// </summary>
    /// <param name="userName">The user name.</param>
    public async Task CreateUserAsync(string userName)
    {
        var userInfo = new UserInfo(null, null, []);
        await _managementClient.CreateUserAsync(userName, userInfo);
        var permissionInfo = new PermissionInfo();
        await _managementClient.CreatePermissionAsync("/", userName, permissionInfo);
    }

    public async Task<bool> CheckConnectionAsync(string containerId, bool checkOpened = true)
    {
        bool rv = true;

        IReadOnlyList<Connection> connections = await _managementClient.GetConnectionsAsync();
        rv = connections.Any(conn =>
                {
                    if (conn.ClientProperties is not null)
                    {
                        if (conn.ClientProperties.TryGetValue("connection_name", out object? connectionNameObj))
                        {
                            if (connectionNameObj is not null)
                            {
                                string connName = (string)connectionNameObj;
                                return connName.Contains(containerId);
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

    public async Task<int> KillConnectionAsync(string containerId)
    {
        IReadOnlyList<Connection> connections = await _managementClient.GetConnectionsAsync();

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
                        return connName.Contains(containerId);
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
                await _managementClient.CloseConnectionAsync(conn);
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

    public Task<Queue> GetQueueAsync(string queueNameStr)
    {
        var queueName = new QueueName(queueNameStr, "/");
        return _managementClient.GetQueueAsync(queueName);
    }

    public async Task<bool> CheckExchangeAsync(string exchangeNameStr, bool checkExisting = true)
    {
        // Assume success
        bool rv = true;

        var exchangeName = new ExchangeName(exchangeNameStr, "/");
        try
        {
            Exchange? exchange = await _managementClient.GetExchangeAsync(exchangeName);
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

    public async Task<bool> CheckQueueAsync(string queueNameStr, bool checkExisting = true)
    {
        // Assume success
        bool rv = true;

        var queueName = new QueueName(queueNameStr, "/");
        try
        {
            Queue? queue = await _managementClient.GetQueueAsync(queueName);
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

    public async Task<bool> CheckBindingsBetweenExchangeAndQueueAsync(string exchangeNameStr, string queueNameStr,
        Dictionary<string, object>? args = null, bool checkExisting = true)
    {
        // Assume success
        bool rv = true;

        var exchangeName = new ExchangeName(exchangeNameStr, "/");
        var queueName = new QueueName(queueNameStr, "/");
        try
        {
            IReadOnlyList<Binding> bindings = await _managementClient.GetQueueBindingsAsync(exchangeName, queueName);
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

    public async Task<bool> CheckBindingsBetweenExchangeAndExchangeAsync(string sourceExchangeNameStr, string destinationExchangeNameStr,
        bool checkExisting = true)
    {
        // Assume success
        bool rv = true;

        var sourceExchangeName = new ExchangeName(sourceExchangeNameStr, "/");
        var destinationExchangeName = new ExchangeName(destinationExchangeNameStr, "/");
        try
        {
            IReadOnlyList<Binding> bindings = await _managementClient.GetExchangeBindingsAsync(sourceExchangeName, destinationExchangeName);
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

    public async Task DeleteExchangeAsync(string exchangeNameStr)
    {
        var exchangeName = new ExchangeName(exchangeNameStr, "/");
        await _managementClient.DeleteExchangeAsync(exchangeName);
    }

    public void Dispose() => _managementClient.Dispose();
}
