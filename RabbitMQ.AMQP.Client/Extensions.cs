// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

#if !NET6_0_OR_GREATER

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    internal static class NetStandardExtensions
    {
        private static readonly TaskContinuationOptions s_tco = TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously;
        private static void IgnoreTaskContinuation(Task t, object s) => t.Exception.Handle(e => true);

        public static void Clear<T>(this ConcurrentBag<T> concurrentBag)
        {
            while (concurrentBag.TryTake(out _))
            {
            }
        }

        public static bool IsCompletedSuccessfully(this Task task)
        {
            return task.Status == TaskStatus.RanToCompletion;
        }

        // https://devblogs.microsoft.com/pfxteam/how-do-i-cancel-non-cancelable-async-operations/
        public static Task WaitAsync(this Task task, CancellationToken cancellationToken)
        {
            if (task.IsCompletedSuccessfully())
            {
                return task;
            }
            else
            {
                return DoWaitAsync(task, cancellationToken);
            }
        }

        // https://devblogs.microsoft.com/pfxteam/how-do-i-cancel-non-cancelable-async-operations/
        public static Task<T> WaitAsync<T>(this Task<T> task, CancellationToken cancellationToken)
        {
            if (task.IsCompletedSuccessfully())
            {
                return task;
            }
            else
            {
                return DoWaitGenericAsync(task, cancellationToken);
            }
        }

        private static async Task DoWaitAsync(this Task task, CancellationToken cancellationToken)
        {
            var cancellationTokenTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            using (cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).TrySetResult(true),
                state: cancellationTokenTcs, useSynchronizationContext: false))
            {
                if (task != await Task.WhenAny(task, cancellationTokenTcs.Task).ConfigureAwait(false))
                {
                    task.Ignore();
                    throw new OperationCanceledException(cancellationToken);
                }
            }

            await task.ConfigureAwait(false);
        }

        private static async Task<T0> DoWaitGenericAsync<T0>(this Task<T0> task, CancellationToken cancellationToken)
        {
            var cancellationTokenTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            using (cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).TrySetResult(true),
                state: cancellationTokenTcs, useSynchronizationContext: false))
            {
                if (task != await Task.WhenAny(task, cancellationTokenTcs.Task).ConfigureAwait(false))
                {
                    task.Ignore();
                    throw new OperationCanceledException(cancellationToken);
                }
            }

            return await task.ConfigureAwait(false);
        }

        public static Task<T> WaitAsync<T>(this Task<T> task, TimeSpan timeout)
        {
            if (task.IsCompletedSuccessfully())
            {
                return task;
            }

            return DoTimeoutAfter(task, timeout);

            // https://github.com/davidfowl/AspNetCoreDiagnosticScenarios/blob/master/AsyncGuidance.md#using-a-timeout
            static async Task<T0> DoTimeoutAfter<T0>(Task<T0> task, TimeSpan timeout)
            {
                using (var cts = new CancellationTokenSource())
                {
                    Task delayTask = Task.Delay(timeout, cts.Token);
                    Task resultTask = await Task.WhenAny(task, delayTask).ConfigureAwait(false);
                    if (resultTask == delayTask)
                    {
                        task.Ignore();
                        throw new TimeoutException();
                    }
                    else
                    {
                        cts.Cancel();
                    }

                    return await task.ConfigureAwait(false);
                }
            }
        }

        public static Task WaitAsync(this Task task, TimeSpan timeout)
        {
            if (task.IsCompletedSuccessfully())
            {
                return task;
            }

            return DoTimeoutAfter(task, timeout);

            // https://github.com/davidfowl/AspNetCoreDiagnosticScenarios/blob/master/AsyncGuidance.md#using-a-timeout
            static async Task DoTimeoutAfter(Task task, TimeSpan timeout)
            {
                using (var cts = new CancellationTokenSource())
                {
                    Task delayTask = Task.Delay(timeout, cts.Token);
                    Task resultTask = await Task.WhenAny(task, delayTask).ConfigureAwait(false);
                    if (resultTask == delayTask)
                    {
                        task.Ignore();
                        throw new TimeoutException();
                    }
                    else
                    {
                        cts.Cancel();
                    }

                    await task.ConfigureAwait(false);
                }
            }
        }

        // https://github.com/dotnet/runtime/issues/23878
        // https://github.com/dotnet/runtime/issues/23878#issuecomment-1398958645
        private static void Ignore(this Task task)
        {
            if (task.IsCompleted)
            {
                _ = task.Exception;
            }
            else
            {
                _ = task.ContinueWith(
                    continuationAction: IgnoreTaskContinuation,
                    state: null,
                    cancellationToken: CancellationToken.None,
                    continuationOptions: s_tco,
                    scheduler: TaskScheduler.Default);
            }
        }
    }
}

#endif
