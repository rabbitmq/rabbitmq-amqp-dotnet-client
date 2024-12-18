// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal interface IVisitor
    {
        Task VisitQueuesAsync(IEnumerable<QueueSpec> queueSpec);
        Task VisitExchangesAsync(IEnumerable<ExchangeSpec> exchangeSpec);
        Task VisitBindingsAsync(IEnumerable<BindingSpec> bindingSpec);
    }

    internal class Visitor : IVisitor
    {
        private readonly AmqpManagement _management;

        internal Visitor(AmqpManagement management)
        {
            _management = management;
        }

        public async Task VisitQueuesAsync(IEnumerable<QueueSpec> queueSpec)
        {
            // TODO this could be done in parallel
            foreach (QueueSpec spec in queueSpec)
            {
                Trace.WriteLine(TraceLevel.Information, $"Recovering queue {spec.QueueName}");
                try
                {
                    await _management.Queue(spec).DeclareAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Trace.WriteLine(TraceLevel.Error,
                        $"Error recovering queue {spec.QueueName}. Error: {e}. Management Status: {_management}");
                }
            }
        }

        public async Task VisitExchangesAsync(IEnumerable<ExchangeSpec> exchangeSpec)
        {
            // TODO this could be done in parallel
            foreach (ExchangeSpec spec in exchangeSpec)
            {
                Trace.WriteLine(TraceLevel.Information, $"Recovering exchange {spec.ExchangeName}");
                try
                {
                    await _management.Exchange(spec).DeclareAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Trace.WriteLine(TraceLevel.Error,
                        $"Error recovering exchange {spec.ExchangeName}. Error: {e}. Management Status: {_management}");
                }
            }
        }

        public async Task VisitBindingsAsync(IEnumerable<BindingSpec> bindingSpec)
        {
            // TODO this could be done in parallel
            foreach (BindingSpec spec in bindingSpec)
            {
                Trace.WriteLine(TraceLevel.Information, $"Recovering binding {spec.BindingPath}");
                try
                {
                    await _management.Binding(spec).BindAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Trace.WriteLine(TraceLevel.Error,
                        $"Error recovering binding {spec.BindingPath}. Error: {e}. Management Status: {_management}");
                }
            }
        }
    }
}
