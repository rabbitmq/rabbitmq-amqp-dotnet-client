// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace RabbitMQ.AMQP.Client;

public interface IConnectionSettings : IEquatable<IConnectionSettings>
{
    string Host { get; }
    int Port { get; }
    string VirtualHost { get; }
    string? User { get; }
    string? Password { get; }
    string Scheme { get; }
    string ContainerId { get; }
    string Path { get; }
    bool UseSsl { get; }
    uint MaxFrameSize { get; }
    SaslMechanism SaslMechanism { get; }
    ITlsSettings? TlsSettings { get; }
    IRecoveryConfiguration Recovery { get; }
}

/// <summary>
/// Contains the TLS/SSL settings for a connection.
/// </summary>
public interface ITlsSettings
{
    /// <summary>
    /// Client certificates to use for mutual authentication.
    /// </summary>
    X509CertificateCollection ClientCertificates { get; }

    /// <summary>
    /// Supported protocols to use.
    /// </summary>
    SslProtocols Protocols { get; set; }

    /// <summary>
    /// Acceptable TLS/SSL errors when connecting.
    /// </summary>
    SslPolicyErrors AcceptablePolicyErrors { get; set; }

    /// <summary>
    /// Specifies whether certificate revocation should be performed during handshake.
    /// </summary>
    bool CheckCertificateRevocation { get; set; }

    /// <summary>
    /// Gets or sets a certificate validation callback to validate remote certificate.
    /// </summary>
    RemoteCertificateValidationCallback? RemoteCertificateValidationCallback { get; set; }

    /// <summary>
    /// Gets or sets a local certificate selection callback to select the certificate which should be used for authentication.
    /// </summary>
    LocalCertificateSelectionCallback? LocalCertificateSelectionCallback { get; set; }
}
