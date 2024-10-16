﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EasyNetQ.Management.Client.Model;
using RabbitMQ.AMQP.Client;
using Xunit.Sdk;

namespace Tests;

public static class SystemUtils
{
    const string DefaultRabbitMqHost = "localhost";
    private static readonly HttpApiClient s_httpApiClient = new();
    private static readonly string s_rabbitMqHost = InitRabbitMqHost();
    private static readonly bool s_isRunningInCI = InitIsRunningInCI();
    private static readonly bool s_isCluster;
    private static readonly ushort s_clusterSize;
    private static readonly TimeSpan s_initialDelaySpan = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan s_shortDelaySpan = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan s_delaySpan = TimeSpan.FromMilliseconds(500);

    public static string RabbitMqHost => s_rabbitMqHost;
    public static bool IsRunningInCI => s_isRunningInCI;
    public static bool IsCluster => s_isCluster;
    public static ushort ClusterSize => s_clusterSize;

    static SystemUtils()
    {
        s_clusterSize = s_httpApiClient.GetClusterSize();
        s_isCluster = s_clusterSize > 1;
    }

    public static EasyNetQ.Management.Client.ManagementClient InitManagementClient()
    {
        ushort managementUriPort = 15672;
        if (IsCluster)
        {
            managementUriPort = (ushort)Utils.RandomNext(15672, 15675);
        }

        Uri managementUri = new($"http://localhost:{managementUriPort}");
        return new EasyNetQ.Management.Client.ManagementClient(managementUri, "guest", "guest");
    }

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

    public static async Task WaitUntilAsync(Func<Task<bool>> func, ushort retries = 20)
    {
        if (s_isRunningInCI)
        {
            retries *= 2;
        }

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

    public static Task WaitUntilConnectionIsOpen(string containerId)
    {
        return WaitUntilAsync(() => s_httpApiClient.CheckConnectionAsync(containerId, checkOpened: true));
    }

    public static Task WaitUntilConnectionIsClosed(string containerId)
    {
        return WaitUntilAsync(() => s_httpApiClient.CheckConnectionAsync(containerId, checkOpened: false));
    }

    public static async Task WaitUntilConnectionIsKilled(string containerId)
    {
        await WaitUntilConnectionIsOpen(containerId);
        await WaitUntilAsync(async () =>
            await s_httpApiClient.KillConnectionAsync(containerId) == 1);
    }

    public static async Task WaitUntilConnectionIsKilledAndOpen(string containerId)
    {
        await WaitUntilConnectionIsOpen(containerId);
        await WaitUntilAsync(async () => await s_httpApiClient.KillConnectionAsync(containerId) == 1);
        await WaitUntilConnectionIsOpen(containerId);
    }

    public static Task WaitUntilQueueExistsAsync(IQueueSpecification queueSpec)
    {
        return WaitUntilQueueExistsAsync(queueSpec.QueueName);
    }

    public static Task WaitUntilQueueExistsAsync(string queueNameStr)
    {
        return WaitUntilAsync(() => { return s_httpApiClient.CheckQueueAsync(queueNameStr, checkExisting: true); });
    }

    public static Task WaitUntilQueueDeletedAsync(IQueueSpecification queueSpec)
    {
        return WaitUntilQueueDeletedAsync(queueSpec.QueueName);
    }

    public static Task WaitUntilQueueDeletedAsync(string queueNameStr)
    {
        return WaitUntilAsync(() => { return s_httpApiClient.CheckQueueAsync(queueNameStr, checkExisting: false); });
    }

    public static Task WaitUntilExchangeExistsAsync(IExchangeSpecification exchangeSpec)
    {
        return WaitUntilExchangeExistsAsync(exchangeSpec.ExchangeName);
    }

    public static Task WaitUntilExchangeExistsAsync(string exchangeNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckExchangeAsync(exchangeNameStr, checkExisting: true);
        });
    }

    public static Task WaitUntilExchangeDeletedAsync(IExchangeSpecification exchangeSpec)
    {
        return WaitUntilExchangeDeletedAsync(exchangeSpec.ExchangeName);
    }

    public static Task WaitUntilExchangeDeletedAsync(string exchangeNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckExchangeAsync(exchangeNameStr, checkExisting: false);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueExistAsync(
        IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueExistAsync(exchangeSpec.ExchangeName, queueSpec.QueueName);
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueExistAsync(string exchangeNameStr, string queueNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                checkExisting: true);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(
        IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(exchangeSpec.ExchangeName, queueSpec.QueueName);
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(string exchangeNameStr,
        string queueNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                checkExisting: false);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(IExchangeSpecification exchangeSpec,
        IQueueSpecification queueSpec,
        Dictionary<string, object> args)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(exchangeSpec.ExchangeName,
            queueSpec.QueueName, args);
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(string exchangeNameStr,
        string queueNameStr,
        Dictionary<string, object> args)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                args: args, checkExisting: true);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(
        IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec,
        Dictionary<string, object> args)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(exchangeSpec.ExchangeName,
            queueSpec.QueueName, args);
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(string exchangeNameStr,
        string queueNameStr,
        Dictionary<string, object> args)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                args: args, checkExisting: false);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(IExchangeSpecification sourceExchangeSpec,
        IExchangeSpecification destinationExchangeSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(sourceExchangeSpec.ExchangeName,
            destinationExchangeSpec.ExchangeName);
    }

    public static Task WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(string sourceExchangeNameStr,
        string destinationExchangeNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndExchangeAsync(sourceExchangeNameStr,
                destinationExchangeNameStr, checkExisting: true);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(
        IExchangeSpecification sourceExchangeSpec, IExchangeSpecification destinationExchangeSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(sourceExchangeSpec.ExchangeName,
            destinationExchangeSpec.ExchangeName);
    }

    public static Task WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(string sourceExchangeNameStr,
        string destinationExchangeNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndExchangeAsync(sourceExchangeNameStr,
                destinationExchangeNameStr, checkExisting: false);
        });
    }

    public static Task WaitUntilQueueMessageCount(IQueueSpecification queueSpec, long messageCount)
    {
        return WaitUntilQueueMessageCount(queueSpec.QueueName, messageCount);
    }

    public static Task WaitUntilQueueMessageCount(string queueNameStr, long messageCount)
    {
        return WaitUntilAsync(async () =>
        {
            Queue queue = await s_httpApiClient.GetQueueAsync(queueNameStr);
            return messageCount == queue.MessagesReady;
        }, retries: 20);
    }

    public static Task DeleteExchangeAsync(string exchangeNameSt)
    {
        return s_httpApiClient.DeleteExchangeAsync(exchangeNameSt);
    }

    private static string InitRabbitMqHost()
    {
        string? envRabbitMqHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST");
        if (false == string.IsNullOrWhiteSpace(envRabbitMqHost))
        {
            return envRabbitMqHost;
        }
        else
        {
            return DefaultRabbitMqHost;
        }
    }

    private static bool InitIsRunningInCI()
    {
        if (bool.TryParse(Environment.GetEnvironmentVariable("CI"), out bool ci))
        {
            if (ci == true)
            {
                return true;
            }
        }
        else if (bool.TryParse(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), out ci))
        {
            if (ci == true)
            {
                return true;
            }
        }

        return false;
    }
}
