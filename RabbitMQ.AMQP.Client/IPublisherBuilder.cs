// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client;

public interface IPublisherBuilder : IAddressBuilder<IPublisherBuilder>
{
    IPublisherBuilder PublishTimeout(TimeSpan timeout);

    IPublisherBuilder MaxInflightMessages(int maxInFlight);

    Task<IPublisher> BuildAsync(CancellationToken cancellationToken = default);
}
