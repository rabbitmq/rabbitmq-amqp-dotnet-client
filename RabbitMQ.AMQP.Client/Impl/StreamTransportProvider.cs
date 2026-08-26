// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// Runs an AMQP connection over a byte stream the application supplies, instead of over a socket
    /// the library opens itself. Used when <see cref="ConnectionSettings.TransportFactory"/> is set.
    /// <para>
    ///   The application is responsible only for producing a connected, plain (not encrypted) duplex
    ///   stream to the broker - for example a socket tunnelled through an HTTP CONNECT proxy. TLS stays
    ///   here, driven by <see cref="ConnectionSettings.TlsSettings"/> and authenticated against
    ///   <see cref="Address.Host"/>, so the broker certificate is validated against the broker's own
    ///   name rather than against whatever address the application had to dial to reach it.
    /// </para>
    /// </summary>
    internal sealed class StreamTransportProvider : TransportProvider
    {
        private readonly ConnectionSettings _connectionSettings;

        /// <summary>
        /// Amqp.TransportProvider.CreateAsync takes no cancellation token, so the token for this
        /// connection attempt is captured here. A provider instance is built per attempt and used
        /// once, so the captured token always belongs to the attempt it serves.
        /// </summary>
        private readonly CancellationToken _cancellationToken;

        internal StreamTransportProvider(ConnectionSettings connectionSettings,
            CancellationToken cancellationToken)
        {
            _connectionSettings = connectionSettings;
            _cancellationToken = cancellationToken;
            AddressSchemes = new[] { "amqp", "amqps" };
        }

        public override async Task<IAsyncTransport> CreateAsync(Address address)
        {
            ConnectionTransportFactory? transportFactory = _connectionSettings.TransportFactory;
            if (transportFactory is null)
            {
                throw new InternalBugException(
                    $"{nameof(StreamTransportProvider)} used without a transport factory" +
                    ", report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }

            Stream stream = await transportFactory(address.Host, address.Port, _cancellationToken)
                .ConfigureAwait(false);

            if (stream is null)
            {
                throw new ConnectionException(
                    $"The transport factory returned no transport for {address.Host}:{address.Port}");
            }

            try
            {
                if (address.UseSsl)
                {
                    // From here on the SslStream owns the stream the factory returned, so disposing
                    // the outer stream on failure releases the underlying socket as well.
                    SslStream sslStream = CreateSslStream(stream);
                    stream = sslStream;
                    await AuthenticateAsClientAsync(sslStream, address.Host)
                        .ConfigureAwait(false);
                }

                return new StreamTransport(stream);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        private SslStream CreateSslStream(Stream innerStream)
        {
            TlsSettings? tlsSettings = _connectionSettings.TlsSettings;
            return new SslStream(innerStream, leaveInnerStreamOpen: false,
                tlsSettings?.RemoteCertificateValidationCallback,
                tlsSettings?.LocalCertificateSelectionCallback);
        }

        private Task AuthenticateAsClientAsync(SslStream sslStream, string targetHost)
        {
            TlsSettings? tlsSettings = _connectionSettings.TlsSettings;
            if (tlsSettings is null)
            {
                return sslStream.AuthenticateAsClientAsync(targetHost);
            }

            return sslStream.AuthenticateAsClientAsync(targetHost, tlsSettings.ClientCertificates,
                tlsSettings.Protocols, tlsSettings.CheckCertificateRevocation);
        }

        /// <summary>
        /// Adapts a <see cref="Stream"/> to the transport interface AMQP.Net Lite drives a connection
        /// through. Modelled on the library's own SslSocket adapter.
        /// </summary>
        private sealed class StreamTransport : IAsyncTransport, IDisposable
        {
            private readonly Stream _stream;

            internal StreamTransport(Stream stream)
            {
                _stream = stream;
            }

            public void Dispose()
            {
                Close();
            }

            public void SetConnection(Connection connection)
            {
                // Nothing to keep: this transport raises no connection-level events of its own.
            }

            public async Task SendAsync(IList<ByteBuffer> bufferList, int listSize)
            {
                // Amqp.TransportWriter serializes its calls into the transport, so writing the
                // buffers one after another preserves frame order.
                for (int i = 0; i < bufferList.Count; i++)
                {
                    ByteBuffer buffer = bufferList[i];
                    await _stream.WriteAsync(buffer.Buffer, buffer.Offset, buffer.Length)
                        .ConfigureAwait(false);
                }
            }

            public Task<int> ReceiveAsync(byte[] buffer, int offset, int count)
            {
                return _stream.ReadAsync(buffer, offset, count);
            }

            public void Send(ByteBuffer buffer)
            {
                _stream.Write(buffer.Buffer, buffer.Offset, buffer.Length);
            }

            public int Receive(byte[] buffer, int offset, int count)
            {
                return _stream.Read(buffer, offset, count);
            }

            public void Close()
            {
                _stream.Dispose();
            }
        }
    }
}
