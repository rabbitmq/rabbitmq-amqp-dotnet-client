// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests
{
    public class AddressBuilderTests
    {
        [Theory]
        [InlineData("myQueue", "/queues/myQueue")]
        [InlineData("queue/with/slash", "/queues/queue%2Fwith%2Fslash")]
        [InlineData("queue with spaces", "/queues/queue%20with%20spaces")]
        [InlineData("queue+with+plus", "/queues/queue%2Bwith%2Bplus")]
        [InlineData("queue?with?question", "/queues/queue%3Fwith%3Fquestion")]
        [InlineData("特殊字符", "/queues/%E7%89%B9%E6%AE%8A%E5%AD%97%E7%AC%A6")]
        [InlineData("emoji😊queue", "/queues/emoji%F0%9F%98%8Aqueue")]
        [InlineData("!@#$%^&*()", "/queues/%21%40%23%24%25%5E%26%2A%28%29")]
        public void AddressBuilder_EncodeAndDecode(string queue, string queuePath)
        {
            AddressBuilder addressBuilder = new();
            string fullAddress = addressBuilder.Queue(queue).Address();
            Assert.Equal(queuePath, fullAddress);
            string decodedQueue = addressBuilder.DecodeQueuePathSegment(fullAddress);
            Assert.Equal(queue, decodedQueue);
        }
    }
}
