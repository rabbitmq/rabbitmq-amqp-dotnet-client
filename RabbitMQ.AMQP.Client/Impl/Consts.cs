// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client.Impl;

public class Consts
{
    public const string Exchanges = "exchanges";
    public const string Key = "key";
    public const string Queues = "queues";
    public const string Bindings = "bindings";

    /// <summary>
    /// <code>uint.MinValue</code> means "no limit"
    /// </summary>
    public const uint DefaultMaxFrameSize = uint.MinValue; // NOTE: Azure/amqpnetlite uses 256 * 1024
}
