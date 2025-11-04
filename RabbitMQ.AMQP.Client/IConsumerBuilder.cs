// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public enum StreamOffsetSpecification
    {
        First,
        Last,
        Next
    }

    // TODO IAddressBuilder<IConsumerBuilder>?
    public interface IConsumerBuilder
    {
        IConsumerBuilder Queue(IQueueSpecification queueSpecification);
        IConsumerBuilder Queue(string? queueName);

        /// <summary>
        /// If direct reply-to is enabled, the client will use the direct reply-to feature of AMQP 1.0.
        /// The server must also support direct reply-to.
        /// This feature allows the server to send the reply directly to the client without going through a reply queue.
        /// This can improve performance and reduce latency.
        /// Default is false.
        /// https://www.rabbitmq.com/docs/direct-reply-to
        /// </summary>

        IConsumerBuilder DirectReplyTo(bool directReplyTo);

        IConsumerBuilder MessageHandler(MessageHandler handler);

        IConsumerBuilder InitialCredits(int initialCredits);

        /// <summary>
        /// SubscriptionListener interface callback to add behavior before a subscription is created.
        /// This callback is meant for stream consumers:
        /// it can be used to dynamically set the offset the consumer attaches to in the stream.
        /// It is called when the consumer is first created and when the client has to re-subscribe
        /// (e.g. after a disconnection).
        /// </summary>
        /// <param name="listenerContext"> Contains the listenerContext, see <see cref="ListenerContext"/>  </param>
        /// <returns><see cref="IConsumerBuilder"/>The consumer builder.</returns>
        IConsumerBuilder SubscriptionListener(Action<ListenerContext> listenerContext);

        IStreamOptions Stream();

        Task<IConsumer> BuildAndStartAsync(CancellationToken cancellationToken = default);

        public interface IStreamOptions
        {
            /// <summary>The offset from which to start consuming.</summary>
            /// <param name="offset">the offset</param>
            /// <returns><see cref="IStreamOptions"/></returns>
            IStreamOptions Offset(long offset);

            /// <summary>
            /// <para>A point in time from which to start consuming.</para>
            /// <para>Be aware consumers can receive messages published a bit before the specified timestamp.</para>
            /// </summary>
            /// <param name="timestamp">the timestamp</param>
            /// <returns><see cref="IStreamOptions"/></returns>
            IStreamOptions Offset(DateTime timestamp);

            /// <summary>The offset from which to start consuming.</summary>
            /// <param name="specification">the offset specification</param>
            /// <returns><see cref="IStreamOptions"/></returns>
            /// <see cref="StreamOffsetSpecification"/>
            IStreamOptions Offset(StreamOffsetSpecification specification);

            /// <summary>
            /// <para>The offset from which to start consuming as an interval string value.</para>
            /// <para>Valid units are Y, M, D, h, m, s. Examples: <code>7D</code> (7 days), <code>12h</code> (12 hours).</para>
            /// </summary>
            /// <param name="interval">the interval</param>
            /// <returns><see cref="IStreamOptions"/></returns>
            /// <see href="https://www.rabbitmq.com/docs/streams#retention">Interval Syntax</see>
            IStreamOptions Offset(string interval);

            /// <summary>
            /// <para>Filter values for stream filtering.</para>
            /// <para>This a different filtering mechanism from AMQP filter expressions. Both mechanisms can be used together.</para>
            /// </summary>
            /// <param name="values">filter values</param>
            /// <returns><see cref="IStreamOptions"/></returns>
            /// <see href="https://www.rabbitmq.com/docs/streams#filtering">Stream Filtering</see>
            /// <see cref="Filter"/>
            IStreamOptions FilterValues(params string[] values);

            /// <summary>
            /// <para>Whether messages without a filter value should be sent.</para>
            /// <para>Default is <code>false</code> (messages without a filter value are not sent).</para>
            /// <para>This a different filtering mechanism from AMQP filter expressions. Both mechanisms can be used together.</para>
            /// </summary>
            /// <param name="matchUnfiltered"><c>true </c>to send messages without a filter value</param>
            /// <returns><see cref="IStreamOptions"/></returns>
            /// @see #filter()
            /// <see cref="Filter"/>
            IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered);

            /// <summary>
            /// <para>Options for AMQP filter expressions.</para>
            /// <para>Requires RabbitMQ 4.1 or more.</para>
            /// <para>This a different filtering mechanism from stream filtering. Both mechanisms can be used together.</para>
            /// </summary>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            /// <see cref="FilterValues"/>
            /// <see cref="FilterMatchUnfiltered(bool)"/>
            IStreamFilterOptions Filter();

            /// <summary>
            /// Return the consumer builder.
            /// </summary>
            /// <returns><see cref="IConsumerBuilder"/></returns>
            IConsumerBuilder Builder();
        }

        /// <summary>
        /// <para>Filter options for support of AMQP filter expressions.</para>
        /// <para>AMQP filter expressions are supported only with streams. This a different filtering mechanism from stream filtering.
        /// Both mechanisms can be used together.
        /// Requires RabbitMQ 4.1 or more.</para>
        /// </summary>
        /// <see href="https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66227">AMQP Filter Expressions</see>
        public interface IStreamFilterOptions
        {
            /// <summary>Filter on message ID.</summary>
            /// <param name="id">message ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions MessageId(object id);

            /// <summary>Filter on user ID.</summary>
            /// <param name="userId">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions UserId(byte[] userId);

            /// <summary>Filter on to field.</summary>
            /// <param name="to">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions To(string to);

            /// <summary>Filter on subject field.</summary>
            /// <param name="subject">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions Subject(string subject);

            /// <summary>Filter on reply-to field.</summary>
            /// <param name="replyTo">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions ReplyTo(string replyTo);

            /// <summary>Filter on correlation ID.</summary>
            /// <param name="correlationId">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions CorrelationId(object correlationId);

            /// <summary>Filter on content-type field.</summary>
            /// <param name="contentType">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions ContentType(string contentType);

            /// <summary>Filter on content-encoding field.</summary>
            /// <param name="contentEncoding">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions ContentEncoding(string contentEncoding);

            /// <summary>Filter on absolute expiry time field.</summary>
            /// <param name="absoluteExpiryTime">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions AbsoluteExpiryTime(DateTime absoluteExpiryTime);

            /// <summary>Filter on creation time field.</summary>
            /// <param name="creationTime">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions CreationTime(DateTime creationTime);

            /// <summary>Filter on group ID.</summary>
            /// <param name="groupId">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions GroupId(string groupId);

            /// <summary>Filter on group sequence.</summary>
            /// <param name="groupSequence">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions GroupSequence(uint groupSequence);

            /// <summary>Filter on reply-to group.</summary>
            /// <param name="groupId">correlation ID</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions ReplyToGroupId(string groupId);

            /// <summary>Filter on an application property.</summary>
            /// <param name="key">application property key</param>
            /// <param name="value">application property value</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions Property(string key, object value);

            /// <summary>Filter on an application property as a <see cref="Amqp.Types.Symbol"/></summary>
            /// <param name="key">application property key</param>
            /// <param name="value">application property value</param>
            /// <returns><see cref="IStreamFilterOptions"/></returns>
            IStreamFilterOptions PropertySymbol(string key, string value);

            /// <summary>
            /// <para>SQL filter expression.</para>
            ///
            /// </summary>
            /// <para>Requires RabbitMQ 4.2 or more.</para>
            /// Documentation: <see href="https://www.rabbitmq.com/docs/next/stream-filtering#sql-filter-expressions">SQL Filtering</see>
            IStreamFilterOptions Sql(string sql);

            /// <summary>
            /// Return the stream options.
            /// </summary>
            /// <returns><see cref="IStreamOptions"/></returns>
            IStreamOptions Stream();
        }

        /// <summary>
        ///  ListenerContext is a helper class that holds the contexts for the listener
        /// </summary>
        public class ListenerContext
        {
            private readonly IStreamOptions _streamOptions;

            /// <param name="streamOptions"> Stream Options that the user can change during the SubscriptionListener </param>
            public ListenerContext(IStreamOptions streamOptions)
            {
                _streamOptions = streamOptions;
            }

            public IStreamOptions StreamOptions => _streamOptions;
        }
    }
}
