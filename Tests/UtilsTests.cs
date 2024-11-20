// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using RabbitMQ.AMQP.Client;
using Xunit;

namespace Tests;

public class UtilsTests
{
    [Theory]
    [InlineData("3.13.6")]
    [InlineData("3.13.6.2")]
    [InlineData("3.13.6-alpha.0")]
    [InlineData("3.13.6~beta-1")]
    [InlineData("3.13.6+funky-metadata-1")]
    [InlineData("4.0.6")]
    [InlineData("4.0.6.9")]
    [InlineData("4.0.6-alpha.0")]
    [InlineData("4.0.6~beta-1")]
    [InlineData("4.0.6+funky-metadata-1")]
    public void BrokerVersionIsParsedAndComparedCorrectly_4_0_0_AndEarlier(string brokerVersion)
    {
        Assert.False(Utils.Is4_1_OrMore(brokerVersion));
        Assert.False(Utils.SupportsFilterExpressions(brokerVersion));
    }

    [Theory]
    [InlineData("4.1.0-alpha.1")]
    [InlineData("4.1.6")]
    [InlineData("4.1.6.9")]
    [InlineData("4.1.6-alpha.0")]
    [InlineData("4.1.6~beta-1")]
    [InlineData("4.1.6+funky-metadata-1")]
    [InlineData("4.2.0-alpha.1")]
    [InlineData("4.2.6")]
    [InlineData("4.2.6.9")]
    [InlineData("4.2.6-alpha.0")]
    [InlineData("4.2.6~beta-1")]
    [InlineData("4.2.6+funky-metadata-1")]
    public void BrokerVersionIsParsedAndComparedCorrectly_4_1_0_AndLater(string brokerVersion)
    {
        Assert.True(Utils.Is4_1_OrMore(brokerVersion));
        Assert.True(Utils.SupportsFilterExpressions(brokerVersion));
    }
}
