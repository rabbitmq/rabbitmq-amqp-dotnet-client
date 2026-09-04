// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

#if !NETFRAMEWORK

using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests.ConnectionTests;

/// <summary>
/// With <c>amqps</c> the library negotiates TLS over the stream the transport factory returned and
/// authenticates the broker against <see cref="ConnectionSettings.Host"/>, not against the address
/// the factory had to dial to get there.
/// <para>
///   Every case below dials the loopback interface and varies only the broker certificate, so the
///   name the platform actually checked shows up in the <see cref="SslPolicyErrors"/> the validation
///   callback is given. No case relaxes validation: the callback records what it was told and then
///   refuses anything that is not <see cref="SslPolicyErrors.None"/>.
/// </para>
/// <para>
///   Not compiled for .NET Framework 4.6.2, which has no <see cref="CertificateRequest"/> to build a
///   test certificate with.
/// </para>
/// </summary>
public class TransportFactoryTlsTests
{
    private const string BrokerHost = "broker-a";
    private const string OtherBrokerHost = "broker-b";
    private const string LoopbackAddress = "127.0.0.1";
    private const int BrokerTlsPort = 5671;
    private const string BrokerCertificateSubject = "CN=broker-cert";
    private const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";

    [Theory]
    // The certificate names the host the settings name, so the name check passes.
    [InlineData(BrokerHost, null, false)]
    // The certificate names a different broker, so the name check fails.
    [InlineData(OtherBrokerHost, null, true)]
    // The certificate names the address the factory dialled, and nothing else. The name check fails
    // all the same, because it is made against the settings host. This is the case that makes the
    // loopback forwarder workaround pass validation without checking the broker's identity.
    [InlineData(null, LoopbackAddress, true)]
    public async Task BrokerCertificateIsValidatedAgainstTheSettingsHost(string? certificateDnsName,
        string? certificateIpAddress, bool expectNameMismatch)
    {
        SslPolicyErrors observedPolicyErrors = SslPolicyErrors.None;
        int validationCallbacks = 0;

        using (X509Certificate2 brokerCertificate =
            CreateBrokerCertificate(certificateDnsName, certificateIpAddress))
        using (var loopbackBroker = new TlsLoopbackBroker(brokerCertificate))
        {
            ConnectionSettings connectionSettings = ConnectionSettingsBuilder.Create()
                .Scheme("amqps")
                .Host(BrokerHost)
                .Port(BrokerTlsPort)
                .ContainerId(nameof(BrokerCertificateIsValidatedAgainstTheSettingsHost))
                .TransportFactory((host, port, cancellationToken) => loopbackBroker.ConnectAsync())
                .Build();

            Assert.True(connectionSettings.UseSsl);
            Assert.NotNull(connectionSettings.TlsSettings);
            Assert.Equal(SslPolicyErrors.None, connectionSettings.TlsSettings.AcceptablePolicyErrors);

            connectionSettings.TlsSettings.RemoteCertificateValidationCallback =
                (sender, certificate, chain, sslPolicyErrors) =>
                {
                    Interlocked.Increment(ref validationCallbacks);
                    observedPolicyErrors = sslPolicyErrors;
                    return sslPolicyErrors == SslPolicyErrors.None;
                };

            await Assert.ThrowsAnyAsync<ConnectionException>(
                async () => await AmqpConnection.CreateAsync(connectionSettings));

            await loopbackBroker.HandshakeCompleted;
        }

        Assert.Equal(1, validationCallbacks);

        // The certificate is self-signed by an issuer this process does not trust, so a chain error
        // is expected in every case. Only the name check is under test here.
        Assert.True(observedPolicyErrors.HasFlag(SslPolicyErrors.RemoteCertificateChainErrors),
            $"expected a chain error, got '{observedPolicyErrors}'");

        bool observedNameMismatch =
            observedPolicyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch);
        Assert.Equal(expectNameMismatch, observedNameMismatch);
    }

    /// <summary>
    /// Builds a self-signed broker certificate carrying exactly the names the case under test needs,
    /// so that what the name check can and cannot match is stated in the test data.
    /// </summary>
    private static X509Certificate2 CreateBrokerCertificate(string? dnsName, string? ipAddress)
    {
        var subjectAlternativeNames = new SubjectAlternativeNameBuilder();
        if (dnsName is not null)
        {
            subjectAlternativeNames.AddDnsName(dnsName);
        }

        if (ipAddress is not null)
        {
            subjectAlternativeNames.AddIpAddress(IPAddress.Parse(ipAddress));
        }

        using (RSA key = RSA.Create(2048))
        {
            var request = new CertificateRequest(BrokerCertificateSubject, key,
                HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

            request.CertificateExtensions.Add(subjectAlternativeNames.Build());
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(
                certificateAuthority: false, hasPathLengthConstraint: false, pathLengthConstraint: 0,
                critical: true));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                new OidCollection { new Oid(ServerAuthenticationOid) }, critical: false));

            DateTimeOffset now = DateTimeOffset.UtcNow;
            using (X509Certificate2 certificate =
                request.CreateSelfSigned(now.AddDays(-1), now.AddDays(1)))
            {
                // Re-importing from PKCS#12 yields a certificate whose private key SslStream can use
                // to complete a server side handshake on every platform.
                return new X509Certificate2(certificate.Export(X509ContentType.Pfx));
            }
        }
    }

    /// <summary>
    /// A TLS listener on the loopback interface that presents one certificate to one client. It never
    /// speaks AMQP: the handshake is the whole of the exchange under test.
    /// </summary>
    private sealed class TlsLoopbackBroker : IDisposable
    {
        private readonly TcpListener _listener;

        /// <summary>Borrowed, not owned: the test that created it disposes it.</summary>
        private readonly X509Certificate2 _serverCertificate;

        private readonly Task _handshakeTask;

        private TcpClient? _client;

        internal TlsLoopbackBroker(X509Certificate2 serverCertificate)
        {
            _serverCertificate = serverCertificate;
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            _handshakeTask = AcceptAndHandshakeAsync();
        }

        /// <summary>
        /// Completes once the server side of the handshake has finished, one way or the other. The
        /// tests await it so that no handshake is left running past the end of a case.
        /// </summary>
        internal Task HandshakeCompleted => _handshakeTask;

        private int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        internal async Task<Stream> ConnectAsync()
        {
            _client = new TcpClient();
            await _client.ConnectAsync(IPAddress.Loopback, Port);
            return _client.GetStream();
        }

        public void Dispose()
        {
            _client?.Dispose();
            _listener.Stop();
        }

        private async Task AcceptAndHandshakeAsync()
        {
            try
            {
                using (TcpClient accepted = await _listener.AcceptTcpClientAsync())
                using (var sslStream = new SslStream(accepted.GetStream(), leaveInnerStreamOpen: false))
                {
                    await sslStream.AuthenticateAsServerAsync(_serverCertificate,
                        clientCertificateRequired: false, checkCertificateRevocation: false);
                }
            }
            catch (Exception)
            {
                // Every case here ends with the client refusing the certificate, which fails the
                // handshake on this side too. What the client was told is what the test asserts on.
            }
        }
    }
}

#endif
