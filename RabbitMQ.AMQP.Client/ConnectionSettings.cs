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
    public interface IUriSelector
    {
        Uri Select(ICollection<Uri> uris);
    }

    public class RandomUriSelector : IUriSelector
    {
        public Uri Select(ICollection<Uri> uris)
        {
            return uris.Skip(Utils.RandomNext(0, uris.Count)).First();
        }
    }

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
        private TlsSettings? _tlsSettings = null;
        private Uri? _uri;
        private List<Uri>? _uris;
        private IUriSelector? _uriSelector;
        private OAuth2Options? _oAuth2Options;

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
            if (Utils.IsValidScheme(scheme))
            {
                _scheme = scheme;
                return this;
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(scheme), "scheme must be 'amqp' or 'amqps'");
            }
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
            if (_maxFrameSize != Consts.DefaultMaxFrameSize && _maxFrameSize < 512)
            {
                throw new ArgumentOutOfRangeException(nameof(maxFrameSize),
                    "maxFrameSize must be 0 (no limit) or greater than or equal to 512");
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

        public ConnectionSettingsBuilder TlsSettings(TlsSettings tlsSettings)
        {
            _tlsSettings = tlsSettings;
            return this;
        }

        public ConnectionSettingsBuilder Uri(Uri uri)
        {
            _uri = uri;
            ValidateUris();
            return this;
        }

        public ConnectionSettingsBuilder Uris(IEnumerable<Uri> uris)
        {
            _uris = uris.ToList();
            ValidateUris();
            return this;
        }

        public ConnectionSettingsBuilder UriSelector(IUriSelector uriSelector)
        {
            _uriSelector = uriSelector;
            return this;
        }

        public ConnectionSettingsBuilder OAuth2Options(OAuth2Options? oAuth2Options)
        {
            _oAuth2Options = oAuth2Options;
            return this;
        }

        public ConnectionSettings Build()
        {
            // TODO this should do something similar to consolidate in the Java code
            ValidateUris();
            if (_uri is not null)
            {
                return new ConnectionSettings(_uri,
                    _containerId, _saslMechanism,
                    _recoveryConfiguration,
                    _maxFrameSize,
                    _tlsSettings,
                    _oAuth2Options);
            }

            if (_uris is not null)
            {
                return new ClusterConnectionSettings(_uris,
                    _uriSelector,
                    _containerId, _saslMechanism,
                    _recoveryConfiguration,
                    _maxFrameSize,
                    _tlsSettings, _oAuth2Options);
            }

            return new ConnectionSettings(_scheme, _host, _port, _user,
                _password, _virtualHost,
                _containerId, _saslMechanism,
                _recoveryConfiguration,
                _maxFrameSize,
                _tlsSettings, _oAuth2Options);
        }

        private void ValidateUris()
        {
            if (_uri is not null && _uris is not null)
            {
                throw new ArgumentOutOfRangeException("uris", "Do not set both Uri and Uris");
            }
        }
    }

    // <summary>
    // Represents a network address.
    // </summary>
    public class ConnectionSettings : IEquatable<ConnectionSettings>
    {
        protected Address _address = new("amqp://localhost:5672");
        protected string _virtualHost = Consts.DefaultVirtualHost;
        private readonly SaslMechanism _saslMechanism = SaslMechanism.Plain;
        private readonly OAuth2Options? _oAuth2Options = null;
        private readonly string _containerId = string.Empty;
        private readonly uint _maxFrameSize = Consts.DefaultMaxFrameSize;
        private readonly TlsSettings? _tlsSettings;
        private readonly IRecoveryConfiguration _recoveryConfiguration = new RecoveryConfiguration();

        public ConnectionSettings(Uri uri,
            string? containerId = null,
            SaslMechanism? saslMechanism = null,
            IRecoveryConfiguration? recoveryConfiguration = null,
            uint? maxFrameSize = null,
            TlsSettings? tlsSettings = null,
            OAuth2Options? oAuth2Options = null)
            : this(containerId, saslMechanism, recoveryConfiguration, maxFrameSize, tlsSettings, oAuth2Options)
        {
            (string? user, string? password) = ProcessUserInfo(uri);

            _virtualHost = ProcessUriSegmentsForVirtualHost(uri);

            string scheme = uri.Scheme;
            if (false == Utils.IsValidScheme(scheme))
            {
                throw new ArgumentOutOfRangeException("uri.Scheme", "Uri scheme must be 'amqp' or 'amqps'");
            }

            _address = InitAddress(uri.Host, uri.Port, user, password, scheme);
            _tlsSettings = InitTlsSettings();
        }

        protected Address InitAddress(string host, int port, string? user, string? password, string scheme)
        {
            if (_oAuth2Options is not null)
            {
                return new Address(host, port, "", _oAuth2Options.Token, "/", scheme);
            }

            return new Address(host,
                port: port,
                user: user,
                password: password,
                path: "/",
                scheme: scheme);
        }

        internal void UpdateOAuthPassword(string? password)
        {
            _address = new Address(_address.Host, _address.Port, _address.User, password, _address.Path, _address.Scheme);
        }

        public ConnectionSettings(string scheme,
            string host,
            int port,
            string? user = null,
            string? password = null,
            string? virtualHost = null,
            string containerId = "",
            SaslMechanism? saslMechanism = null,
            IRecoveryConfiguration? recoveryConfiguration = null,
            uint? maxFrameSize = null,
            TlsSettings? tlsSettings = null,
            OAuth2Options? oAuth2Options = null)
            : this(containerId, saslMechanism, recoveryConfiguration, maxFrameSize, tlsSettings, oAuth2Options)
        {
            if (false == Utils.IsValidScheme(scheme))
            {
                throw new ArgumentOutOfRangeException(nameof(scheme), "scheme must be 'amqp' or 'amqps'");
            }

            _address = InitAddress(host, port, user, password, scheme);
            if (virtualHost is not null)
            {
                _virtualHost = virtualHost;
            }

            _tlsSettings = InitTlsSettings();
        }

        protected ConnectionSettings(
            string? containerId = null,
            SaslMechanism? saslMechanism = null,
            IRecoveryConfiguration? recoveryConfiguration = null,
            uint? maxFrameSize = null,
            TlsSettings? tlsSettings = null,
            OAuth2Options? oAuth2Options = null)
        {
            if (containerId is not null)
            {
                _containerId = containerId;
            }

            if (saslMechanism is not null)
            {
                _saslMechanism = saslMechanism;
            }

            if (oAuth2Options is not null)
            {
                // If OAuth2Options is set, then SaslMechanism must be Plain
                _oAuth2Options = oAuth2Options;
                _saslMechanism = SaslMechanism.Plain;
            }

            if (recoveryConfiguration is not null)
            {
                _recoveryConfiguration = recoveryConfiguration;
            }

            if (maxFrameSize is not null)
            {
                _maxFrameSize = (uint)maxFrameSize;
                if (_maxFrameSize != Consts.DefaultMaxFrameSize && _maxFrameSize < 512)
                {
                    throw new ArgumentOutOfRangeException(nameof(maxFrameSize),
                        "maxFrameSize must be 0 (no limit) or greater than or equal to 512");
                }
            }

            _tlsSettings = tlsSettings;
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

        internal virtual Address Address => _address;

        internal virtual IList<Address> Addresses => new[] { _address };

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

        protected static (string? user, string? password) ProcessUserInfo(Uri uri)
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

            return (user, password);
        }

        protected static string ProcessUriSegmentsForVirtualHost(Uri uri)
        {
            // C# automatically changes URIs into a canonical form
            // that has at least the path segment "/"
            if (uri.Segments.Length > 2)
            {
                throw new ArgumentException(
                    $"Multiple segments in path of AMQP URI: {string.Join(", ", uri.Segments)}");
            }

            if (uri.Segments.Length == 2)
            {
                return UriDecode(uri.Segments[1]);
            }
            else
            {
                return Consts.DefaultVirtualHost;
            }
        }

        private TlsSettings? InitTlsSettings()
        {
            if (_address.UseSsl && _tlsSettings is null)
            {
                return new TlsSettings();
            }
            else
            {
                return null;
            }
        }

        ///<summary>
        /// Unescape a string, protecting '+'.
        /// </summary>
        private static string UriDecode(string str)
        {
            return Uri.UnescapeDataString(str.Replace("+", "%2B"));
        }
    }

    public class ClusterConnectionSettings : ConnectionSettings
    {
        private readonly List<Uri> _uris;
        private readonly Dictionary<Uri, Address> _uriToAddress;
        private readonly IUriSelector _uriSelector = new RandomUriSelector();

        public ClusterConnectionSettings(IEnumerable<Uri> uris,
            IUriSelector? uriSelector = null,
            string? containerId = null,
            SaslMechanism? saslMechanism = null,
            IRecoveryConfiguration? recoveryConfiguration = null,
            uint? maxFrameSize = null,
            TlsSettings? tlsSettings = null,
            OAuth2Options? oAuth2Options = null)
            : base(containerId, saslMechanism, recoveryConfiguration, maxFrameSize, tlsSettings, oAuth2Options)
        {
            _uris = uris.ToList();
            if (_uris.Count == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(uris), "At least one Uri is required.");
            }

            _uriToAddress = new Dictionary<Uri, Address>(_uris.Count);

            if (uriSelector is not null)
            {
                _uriSelector = uriSelector;
            }

            string? tmpVirtualHost = null;

            bool first = true;
            foreach (Uri uri in _uris)
            {
                string scheme = uri.Scheme;
                if (false == Utils.IsValidScheme(scheme))
                {
                    throw new ArgumentOutOfRangeException("uri.Scheme", "Uri scheme must be 'amqp' or 'amqps'");
                }

                (string? user, string? password) = ProcessUserInfo(uri);

                if (tmpVirtualHost is null)
                {
                    tmpVirtualHost = ProcessUriSegmentsForVirtualHost(uri);
                }
                else
                {
                    string thisVirtualHost = ProcessUriSegmentsForVirtualHost(uri);
                    if (false == thisVirtualHost.Equals(tmpVirtualHost, StringComparison.InvariantCultureIgnoreCase))
                    {
                        throw new ArgumentException(
                            $"All AMQP URIs must use the same virtual host. Expected '{tmpVirtualHost}', got '{thisVirtualHost}'");
                    }
                }

                var address = InitAddress(uri.Host, uri.Port, user, password, scheme);

                _uriToAddress[uri] = address;

                if (first)
                {
                    _address = address;
                    first = false;
                }
            }

            if (tmpVirtualHost is not null)
            {
                _virtualHost = tmpVirtualHost;
            }
        }

        public override bool Equals(object? obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (base.Equals(obj) && (obj is ClusterConnectionSettings other))
            {
                for (int i = 0; i < _uris.Count; i++)
                {
                    Uri thisUri = _uris[i];
                    Uri otherUri = other._uris[i];
                    if (false == thisUri.Equals(otherUri))
                    {
                        return false;
                    }
                }

                return true;
            }

            return false;
        }

        public override int GetHashCode()
        {
            int baseHashCode = base.GetHashCode();
            int hashCode = baseHashCode;
            for (int i = 0; i < _uris.Count; i++)
            {
                hashCode ^= _uris[i].GetHashCode();
            }

            return hashCode;
        }

        internal override Address Address
        {
            get
            {
                Uri uri = _uriSelector.Select(_uris);
                return _uriToAddress[uri];
            }
        }

        internal override IList<Address> Addresses => _uriToAddress.Values.ToList();
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
            RemoteCertificateValidationCallback = TrustEverythingCertValidationCallback;
            LocalCertificateSelectionCallback = null;
        }

        public SslProtocols Protocols { get; set; }

        public SslPolicyErrors AcceptablePolicyErrors { get; set; } = SslPolicyErrors.None;

        public X509CertificateCollection ClientCertificates => _clientCertificates;

        public bool CheckCertificateRevocation { get; set; } = false;

        public RemoteCertificateValidationCallback? RemoteCertificateValidationCallback { get; set; }

        public LocalCertificateSelectionCallback? LocalCertificateSelectionCallback { get; set; }

        private bool TrustEverythingCertValidationCallback(object sender, X509Certificate? certificate,
            X509Chain? chain, SslPolicyErrors sslPolicyErrors)
        {
            return (sslPolicyErrors & ~AcceptablePolicyErrors) == SslPolicyErrors.None;
        }
    }

    public class OAuth2Options
    {
        public OAuth2Options(string token)
        {
            Token = token;
        }

        public string Token { get; set; }
    }
}
