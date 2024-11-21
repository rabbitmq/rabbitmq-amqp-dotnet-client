// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Amqp;

namespace RabbitMQ.AMQP.Client
{
    public class ConnectionSettingsBuilder
    {
        private string _host = "localhost";
        private int _port = -1; // Note: -1 means use the defalt for the scheme
        private string? _user = "guest";
        private string? _password = "guest";
        private string _scheme = "AMQP";
        private string _containerId = "AMQP.NET";
        private string _virtualHost = "/";
        private uint _maxFrameSize = Consts.DefaultMaxFrameSize;
        private SaslMechanism _saslMechanism = Client.SaslMechanism.Anonymous;
        private IRecoveryConfiguration _recoveryConfiguration = new RecoveryConfiguration();
        private IList<Uri>? _uris;

        public static ConnectionSettingsBuilder Create()
        {
            return new ConnectionSettingsBuilder();
        }

        public ConnectionSettingsBuilder Host(string host)
        {
            _host = host;
            return this;
        }

        public ConnectionSettingsBuilder Port(int port)
        {
            _port = port;
            return this;
        }

        public ConnectionSettingsBuilder User(string user)
        {
            _user = user;
            return this;
        }

        public ConnectionSettingsBuilder Password(string password)
        {
            _password = password;
            return this;
        }

        public ConnectionSettingsBuilder Scheme(string scheme)
        {
            _scheme = scheme;
            return this;
        }

        public ConnectionSettingsBuilder ContainerId(string containerId)
        {
            _containerId = containerId;
            return this;
        }

        public ConnectionSettingsBuilder VirtualHost(string virtualHost)
        {
            _virtualHost = virtualHost;
            return this;
        }

        public ConnectionSettingsBuilder MaxFrameSize(uint maxFrameSize)
        {
            _maxFrameSize = maxFrameSize;
            if (_maxFrameSize != uint.MinValue && _maxFrameSize < 512)
            {
                throw new ArgumentOutOfRangeException(nameof(maxFrameSize),
                    "maxFrameSize must be greater or equal to 512");
            }
            return this;
        }

        public ConnectionSettingsBuilder SaslMechanism(SaslMechanism saslMechanism)
        {
            _saslMechanism = saslMechanism;
            if (_saslMechanism == Client.SaslMechanism.Anonymous ||
                _saslMechanism == Client.SaslMechanism.External)
            {
                _user = null;
                _password = null;
            }

            return this;
        }

        public ConnectionSettingsBuilder RecoveryConfiguration(IRecoveryConfiguration recoveryConfiguration)
        {
            _recoveryConfiguration = recoveryConfiguration;
            return this;
        }

        public ConnectionSettingsBuilder Uris(IEnumerable<Uri> uris)
        {
            _uris = uris.ToList();
            return this;
        }

        public ConnectionSettings Build()
        {
            // TODO this should do something similar to consolidate in the Java code
            var c = new ConnectionSettings(_scheme, _host, _port, _user,
                _password, _virtualHost,
                _containerId, _saslMechanism,
                _recoveryConfiguration,
                _maxFrameSize);
            return c;
        }
    }

    // <summary>
    // Represents a network address.
    // </summary>
    public class ConnectionSettings : IEquatable<ConnectionSettings>
    {
        private readonly Address _address;
        private readonly string _virtualHost = "/";
        private readonly string _containerId = "";
        private readonly uint _maxFrameSize = Consts.DefaultMaxFrameSize;
        private readonly TlsSettings? _tlsSettings;
        private readonly SaslMechanism _saslMechanism = SaslMechanism.Plain;
        private readonly IRecoveryConfiguration _recoveryConfiguration = new RecoveryConfiguration();

        public ConnectionSettings(Uri uri)
        {
            string? user = null;
            string? password = null;
            string userInfo = uri.UserInfo;
            if (!string.IsNullOrEmpty(userInfo))
            {
                string[] userPass = userInfo.Split(':');
                if (userPass.Length > 2)
                {
                    throw new ArgumentException($"Bad user info in AMQP URI: {userInfo}");
                }

                user = UriDecode(userPass[0]);
                if (userPass.Length == 2)
                {
                    password = UriDecode(userPass[1]);
                }
            }

            // C# automatically changes URIs into a canonical form
            // that has at least the path segment "/"
            if (uri.Segments.Length > 2)
            {
                throw new ArgumentException($"Multiple segments in path of AMQP URI: {string.Join(", ", uri.Segments)}");
            }

            if (uri.Segments.Length == 2)
            {
                _virtualHost = UriDecode(uri.Segments[1]);
            }

            _address = new Address(host: uri.Host,
                port: uri.Port,
                user: user,
                password: password,
                path: "/",
                scheme: uri.Scheme);

            if (_address.UseSsl && _tlsSettings == null)
            {
                _tlsSettings = new TlsSettings();
            }
        }

        public ConnectionSettings(string scheme, string host, int port,
            string? user, string? password,
            string virtualHost, string containerId,
            SaslMechanism saslMechanism,
            IRecoveryConfiguration recoveryConfiguration,
            uint maxFrameSize = Consts.DefaultMaxFrameSize,
            TlsSettings? tlsSettings = null)
        {
            _address = new Address(host: host, port: port,
                user: user, password: password,
                path: "/", scheme: scheme);
            _containerId = containerId;
            _virtualHost = virtualHost;
            _saslMechanism = saslMechanism;

            _maxFrameSize = maxFrameSize;
            if (_maxFrameSize != uint.MinValue && _maxFrameSize < 512)
            {
                throw new ArgumentOutOfRangeException(nameof(maxFrameSize),
                    "maxFrameSize must be greater or equal to 512");
            }

            _tlsSettings = tlsSettings;

            if (_address.UseSsl && _tlsSettings == null)
            {
                _tlsSettings = new TlsSettings();
            }

            _recoveryConfiguration = recoveryConfiguration;
        }

        public string Host => _address.Host;
        public int Port => _address.Port;
        public string VirtualHost => _virtualHost;
        public string? User => _address.User;
        public string? Password => _address.Password;
        public string Scheme => _address.Scheme;
        public string ContainerId => _containerId;
        public string Path => _address.Path;
        public bool UseSsl => _address.UseSsl;
        public uint MaxFrameSize => _maxFrameSize;
        public SaslMechanism SaslMechanism => _saslMechanism;
        public TlsSettings? TlsSettings => _tlsSettings;
        public IRecoveryConfiguration Recovery => _recoveryConfiguration;
        public IEnumerable<Uri>? Uris => throw new NotImplementedException();

        internal Address Address => _address;

        public override string ToString()
        {
            return
                $"Address" +
                $"host='{_address.Host}', " +
                $"port={_address.Port}, VirtualHost='{_virtualHost}', path='{_address.Path}', " +
                $"username='{_address.User}', ContainerId='{_containerId}'";
        }

        public override bool Equals(object? obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (Object.ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj is ConnectionSettings other)
            {
                return
                    _address.Host == other._address.Host &&
                    _address.Port == other._address.Port &&
                    _virtualHost == other._virtualHost &&
                    _address.User == other._address.User &&
                    _address.Password == other._address.Password &&
                    _address.Scheme == other._address.Scheme &&
                    _containerId == other._containerId &&
                    _address.Path == other._address.Path;
            }

            return false;
        }

        bool IEquatable<ConnectionSettings>.Equals(ConnectionSettings? other)
        {
            if (other is null)
            {
                return false;
            }

            if (Object.ReferenceEquals(this, other))
            {
                return true;
            }

            return Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(_address.Host, _address.Port,
                _virtualHost, _address.User, _address.Password,
                _address.Scheme, _containerId, _address.Path);
        }

        ///<summary>
        /// Unescape a string, protecting '+'.
        /// </summary>
        private static string UriDecode(string str)
        {
            return Uri.UnescapeDataString(str.Replace("+", "%2B"));
        }
    }

    public class TlsSettings
    {
        internal const SslProtocols DefaultSslProtocols = SslProtocols.None;
        private readonly X509CertificateCollection _clientCertificates = new X509CertificateCollection();

        public TlsSettings() : this(DefaultSslProtocols)
        {
        }

        public TlsSettings(SslProtocols protocols)
        {
            Protocols = protocols;
            RemoteCertificateValidationCallback = trustEverythingCertValidationCallback;
            LocalCertificateSelectionCallback = null;
        }

        public SslProtocols Protocols { get; set; }

        public SslPolicyErrors AcceptablePolicyErrors { get; set; } = SslPolicyErrors.None;

        public X509CertificateCollection ClientCertificates => _clientCertificates;

        public bool CheckCertificateRevocation { get; set; } = false;

        public RemoteCertificateValidationCallback? RemoteCertificateValidationCallback { get; set; }

        public LocalCertificateSelectionCallback? LocalCertificateSelectionCallback { get; set; }

        private bool trustEverythingCertValidationCallback(object sender, X509Certificate? certificate,
            X509Chain? chain, SslPolicyErrors sslPolicyErrors)
        {
            return (sslPolicyErrors & ~AcceptablePolicyErrors) == SslPolicyErrors.None;
        }
    }
}
