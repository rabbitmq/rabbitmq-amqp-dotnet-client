// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using Amqp.Types;

namespace RabbitMQ.AMQP.Client
{
    public static class Consts
    {
        public const string Exchanges = "exchanges";
        public const string Key = "key";
        public const string Queues = "queues";
        public const string Bindings = "bindings";
        public const string Messages = "messages";

        /// <summary>
        /// <code>uint.MinValue</code> means "no limit"
        /// </summary>
        public const uint DefaultMaxFrameSize = uint.MinValue; // NOTE: Azure/amqpnetlite uses 256 * 1024

        /// <summary>
        /// The default virtual host, <c>/</c>
        /// </summary>
        public const string DefaultVirtualHost = "/";

        // amqp:sql-filter
        private const string AmqpSqlFilter = "amqp:sql-filter";
        internal static readonly Symbol s_streamSqlFilterSymbol = new(AmqpSqlFilter);
        internal const string SqlFilter = "sql-filter";

        internal const string AmqpPropertiesFilter = "amqp:properties-filter";
        internal const string AmqpApplicationPropertiesFilter = "amqp:application-properties-filter";

        // sql-filter
        private const string RmqStreamFilter = "rabbitmq:stream-filter";
        private const string RmqStreamOffsetSpec = "rabbitmq:stream-offset-spec";
        private const string RmqStreamMatchUnfiltered = "rabbitmq:stream-match-unfiltered";

        internal static readonly Symbol s_streamFilterSymbol = new(RmqStreamFilter);
        internal static readonly Symbol s_streamOffsetSpecSymbol = new(RmqStreamOffsetSpec);
        internal static readonly Symbol s_streamMatchUnfilteredSymbol = new(RmqStreamMatchUnfiltered);

    }
}
