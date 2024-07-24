using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class ConnectionSettingBuilder
{
    // TODO: maybe add the event "LifeCycle" to the builder
    private string _host = "localhost";
    private int _port = -1; // Note: -1 means use the defalt for the scheme
    private string _user = "guest";
    private string _password = "guest";
    private string _scheme = "AMQP";
    private string _connection = "AMQP.NET";
    private string _virtualHost = "/";
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

    public ConnectionSettingBuilder ConnectionName(string connection)
    {
        _connection = connection;
        return this;
    }

    public ConnectionSettingBuilder VirtualHost(string virtualHost)
    {
        _virtualHost = virtualHost;
        return this;
    }

    public ConnectionSettingBuilder RecoveryConfiguration(IRecoveryConfiguration recoveryConfiguration)
    {
        _recoveryConfiguration = recoveryConfiguration;
        return this;
    }

    public ConnectionSettings Build()
    {
        var c = new ConnectionSettings(_host, _port, _user,
            _password, _virtualHost,
            _scheme, _connection)
        {
            RecoveryConfiguration = (RecoveryConfiguration)_recoveryConfiguration
        };

        return c;
    }
}

// <summary>
// Represents a network address.
// </summary>
public class ConnectionSettings : IConnectionSettings
{
    private readonly Address _address;
    private readonly string _connectionName = "";
    private readonly string _virtualHost = "/";
    private readonly ITlsSettings? _tlsSettings;

    public ConnectionSettings(string address, ITlsSettings? tlsSettings = null)
    {
        _address = new Address(address);
        _tlsSettings = tlsSettings;

        if (_address.UseSsl && _tlsSettings == null)
        {
            _tlsSettings = new TlsSettings();
        }
    }

    public ConnectionSettings(string host, int port,
        string user, string password,
        string virtualHost, string scheme, string connectionName,
        ITlsSettings? tlsSettings = null)
    {
        _address = new Address(host: host, port: port,
            user: user, password: password,
            path: "/", scheme: scheme);
        _connectionName = connectionName;
        _virtualHost = virtualHost;
        _tlsSettings = tlsSettings;

        if (_address.UseSsl && _tlsSettings == null)
        {
            _tlsSettings = new TlsSettings();
        }
    }

    public string Host => _address.Host;
    public int Port => _address.Port;
    public string VirtualHost => _virtualHost;
    public string User => _address.User;
    public string Password => _address.Password;
    public string Scheme => _address.Scheme;
    public string ConnectionName => _connectionName;
    public string Path => _address.Path;
    public bool UseSsl => _address.UseSsl;

    public ITlsSettings? TlsSettings => _tlsSettings;

    public override string ToString()
    {
        return
            $"Address" +
            $"host='{_address.Host}', " +
            $"port={_address.Port}, VirtualHost='{_virtualHost}', path='{_address.Path}', " +
            $"username='{_address.User}', ConnectionName='{_connectionName}'";
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

    public RecoveryConfiguration RecoveryConfiguration { get; set; } = RecoveryConfiguration.Create();
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

    private BackOffDelayPolicy()
    {
    }

    private const int StartRandomMilliseconds = 500;
    private const int EndRandomMilliseconds = 1500;

    private int _attempt = 1;
    private int _totalAttempt = 0;

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
        _totalAttempt++;
        ResetAfterMaxAttempt();
        return Random.Shared.Next(StartRandomMilliseconds, EndRandomMilliseconds) * _attempt;
    }

    public void Reset()
    {
        _attempt = 1;
        _totalAttempt = 0;
    }

    public bool IsActive()
    {
        return _totalAttempt < 12;
    }


    public override string ToString()
    {
        return $"BackOffDelayPolicy{{ Attempt={_attempt}, TotalAttempt={_totalAttempt}, IsActive={IsActive} }}";
    }
}

public class TlsSettings : ITlsSettings
{
    internal const SslProtocols DefaultSslProtocols = SslProtocols.None;

    private readonly SslProtocols _protocols;
    private readonly X509CertificateCollection _clientCertificates;
    private readonly bool _checkCertificateRevocation = false;
    private readonly RemoteCertificateValidationCallback? _remoteCertificateValidationCallback;
    private readonly LocalCertificateSelectionCallback? _localCertificateSelectionCallback;

    public TlsSettings() : this(DefaultSslProtocols)
    {
    }

    public TlsSettings(SslProtocols protocols)
    {
        _protocols = protocols;
        _clientCertificates = new X509CertificateCollection();
        _remoteCertificateValidationCallback = trustEverythingCertValidationCallback;
        _localCertificateSelectionCallback = null;
    }

    public SslProtocols Protocols => _protocols;

    public X509CertificateCollection ClientCertificates => _clientCertificates;

    public bool CheckCertificateRevocation => _checkCertificateRevocation;

    public RemoteCertificateValidationCallback? RemoteCertificateValidationCallback
        => _remoteCertificateValidationCallback;

    public LocalCertificateSelectionCallback? LocalCertificateSelectionCallback
        => _localCertificateSelectionCallback;

    private static bool trustEverythingCertValidationCallback(object sender, X509Certificate? certificate,
        X509Chain? chain, SslPolicyErrors sslPolicyErrors)
    {
        return true;
    }
}
