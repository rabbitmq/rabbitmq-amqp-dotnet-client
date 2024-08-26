// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class ConnectionSettingBuilder
    {
        // TODO: maybe add the event "LifeCycle" to the builder
        private string _host = "localhost";
        private int _port = -1; // Note: -1 means use the defalt for the scheme
        private string? _user = "guest";
        private string? _password = "guest";
        private string _scheme = "AMQP";
        private string _containerId = "AMQP.NET";
        private string _virtualHost = "/";
        private uint _maxFrameSize = Consts.DefaultMaxFrameSize;
        private SaslMechanism _saslMechanism = Client.SaslMechanism.Plain;
        private IRecoveryConfiguration _recoveryConfiguration = Impl.RecoveryConfiguration.Create();

        private ConnectionSettingBuilder()
        {
        }

        public static ConnectionSettingBuilder Create()
        {
            return new ConnectionSettingBuilder();
        }

        public ConnectionSettingBuilder Host(string host)
        {
            _host = host;
            return this;
        }

        public ConnectionSettingBuilder Port(int port)
        {
            _port = port;
            return this;
        }

        public ConnectionSettingBuilder User(string user)
        {
            _user = user;
            return this;
        }

        public ConnectionSettingBuilder Password(string password)
        {
            _password = password;
            return this;
        }

        public ConnectionSettingBuilder Scheme(string scheme)
        {
            _scheme = scheme;
            return this;
        }

        public ConnectionSettingBuilder ContainerId(string containerId)
        {
            _containerId = containerId;
            return this;
        }

        public ConnectionSettingBuilder VirtualHost(string virtualHost)
        {
            _virtualHost = virtualHost;
            return this;
        }

        public ConnectionSettingBuilder MaxFrameSize(uint maxFrameSize)
        {
            _maxFrameSize = maxFrameSize;
            if (_maxFrameSize != uint.MinValue && _maxFrameSize < 512)
            {
                throw new ArgumentOutOfRangeException(nameof(maxFrameSize),
                    "maxFrameSize must be greater or equal to 512");
            }
            return this;
        }

        public ConnectionSettingBuilder SaslMechanism(SaslMechanism saslMechanism)
        {
            _saslMechanism = saslMechanism;
            if (_saslMechanism == Client.SaslMechanism.External)
            {
                _user = null;
                _password = null;
            }

            return this;
        }

        public ConnectionSettingBuilder RecoveryConfiguration(IRecoveryConfiguration recoveryConfiguration)
        {
            _recoveryConfiguration = recoveryConfiguration;
            return this;
        }

        public ConnectionSettings Build()
        {
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
    public class ConnectionSettings : IConnectionSettings
    {
        private readonly Address _address;
        private readonly string _virtualHost = "/";
        private readonly string _containerId = "";
        private readonly uint _maxFrameSize = Consts.DefaultMaxFrameSize;
        private readonly ITlsSettings? _tlsSettings;
        private readonly SaslMechanism _saslMechanism = SaslMechanism.Plain;
        private readonly IRecoveryConfiguration _recoveryConfiguration = RecoveryConfiguration.Create();

        public ConnectionSettings(string address, ITlsSettings? tlsSettings = null)
        {
            _address = new Address(address);
            _tlsSettings = tlsSettings;

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
            ITlsSettings? tlsSettings = null)
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
        public ITlsSettings? TlsSettings => _tlsSettings;
        public IRecoveryConfiguration Recovery => _recoveryConfiguration;

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

            if (obj is ConnectionSettings address)
            {
                return _address.Host == address._address.Host &&
                       _address.Port == address._address.Port &&
                       _address.Path == address._address.Path &&
                       _address.User == address._address.User &&
                       _address.Password == address._address.Password &&
                       _address.Scheme == address._address.Scheme;
            }

            return false;
        }

        protected bool Equals(ConnectionSettings other)
        {
            if (other is null)
            {
                return false;
            }

            return _address.Equals(other._address);
        }

        public override int GetHashCode()
        {
            return _address.GetHashCode();
        }

        public bool Equals(IConnectionSettings? other)
        {
            if (other is null)
            {
                return false;
            }

            if (other is IConnectionSettings connectionSettings)
            {
                return _address.Host == connectionSettings.Host &&
                       _address.Port == connectionSettings.Port &&
                       _address.Path == connectionSettings.Path &&
                       _address.User == connectionSettings.User &&
                       _address.Password == connectionSettings.Password &&
                       _address.Scheme == connectionSettings.Scheme;
            }

            return false;
        }

        internal Address Address => _address;

        // public RecoveryConfiguration RecoveryConfiguration { get; set; } = RecoveryConfiguration.Create();
    }

    /// <summary>
    /// RecoveryConfiguration is a class that represents the configuration of the recovery of the topology.
    /// It is used to configure the recovery of the topology of the server after a connection is established in case of a reconnection
    /// The RecoveryConfiguration can be disabled or enabled.
    /// If RecoveryConfiguration._active is disabled, the reconnect mechanism will not be activated.
    /// If RecoveryConfiguration._topology is disabled, the recovery of the topology will not be activated.
    /// </summary>
    public class RecoveryConfiguration : IRecoveryConfiguration
    {
        public static RecoveryConfiguration Create()
        {
            return new RecoveryConfiguration();
        }

        private RecoveryConfiguration()
        {
        }

        // Activate the reconnect mechanism
        private bool _active = true;

        // Activate the recovery of the topology
        private bool _topology = false;

        private IBackOffDelayPolicy _backOffDelayPolicy = Impl.BackOffDelayPolicy.Create();

        public IRecoveryConfiguration Activated(bool activated)
        {
            _active = activated;
            return this;
        }

        public bool IsActivate()
        {
            return _active;
        }

        public IRecoveryConfiguration BackOffDelayPolicy(IBackOffDelayPolicy backOffDelayPolicy)
        {
            _backOffDelayPolicy = backOffDelayPolicy;
            return this;
        }

        public IBackOffDelayPolicy GetBackOffDelayPolicy()
        {
            return _backOffDelayPolicy;
        }

        public IRecoveryConfiguration Topology(bool activated)
        {
            _topology = activated;
            return this;
        }

        public bool IsTopologyActive()
        {
            return _topology;
        }

        public override string ToString()
        {
            return
                $"RecoveryConfiguration{{ Active={_active}, Topology={_topology}, BackOffDelayPolicy={_backOffDelayPolicy} }}";
        }
    }

    public class BackOffDelayPolicy : IBackOffDelayPolicy
    {
        public static BackOffDelayPolicy Create()
        {
            return new BackOffDelayPolicy();
        }

        public static BackOffDelayPolicy Create(int maxAttempt)
        {
            return new BackOffDelayPolicy(maxAttempt);
        }

        private BackOffDelayPolicy()
        {
        }

        private BackOffDelayPolicy(int maxAttempt)
        {
            _maxAttempt = maxAttempt;
        }

        private const int StartRandomMilliseconds = 500;
        private const int EndRandomMilliseconds = 1500;

        private int _attempt = 1;
        private readonly int _maxAttempt = 12;


        private void ResetAfterMaxAttempt()
        {
            if (_attempt > 5)
            {
                _attempt = 1;
            }
        }

        public int Delay()
        {
            _attempt++;
            CurrentAttempt++;
            ResetAfterMaxAttempt();
            return Utils.RandomNext(StartRandomMilliseconds, EndRandomMilliseconds) * _attempt;
        }

        public void Reset()
        {
            _attempt = 1;
            CurrentAttempt = 0;
        }

        public bool IsActive()
        {
            return CurrentAttempt < _maxAttempt;
        }

        public int CurrentAttempt { get; private set; } = 0;


        public override string ToString()
        {
            return $"BackOffDelayPolicy{{ Attempt={_attempt}, TotalAttempt={CurrentAttempt}, IsActive={IsActive()} }}";
        }
    }

    public class TlsSettings : ITlsSettings
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
