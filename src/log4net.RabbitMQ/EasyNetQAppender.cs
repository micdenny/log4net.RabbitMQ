using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using EasyNetQ;
using EasyNetQ.Loggers;
using EasyNetQ.Topology;
using log4net.Appender;
using log4net.Core;
using RabbitMQ.Client;

namespace log4net.RabbitMQ
{
    /// <summary>
    /// An appender for log4net that publishes to RabbitMQ.
    /// </summary>
    public class EasyNetQAppender : AppenderSkeleton
    {
        private ExchangeProperties _ExchangeProperties = new ExchangeProperties();
        private MessageProperties _MessageProperties = new MessageProperties();

        private IBus _Bus;
        private readonly Encoding _Encoding = Encoding.UTF8;
        private readonly DateTime _Epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);

        #region Properties

        private string _VHost = "/";

        /// <summary>
        /// Gets or sets the virtual host to publish to.
        /// </summary>
        public string VHost
        {
            get { return _VHost; }
            set
            {
                if (value != null) _VHost = value;
            }
        }

        private string _UserName = "guest";

        /// <summary>
        /// Gets or sets the username to use for
        /// authentication with the message broker. The default
        /// is 'guest'
        /// </summary>
        public string UserName
        {
            get { return _UserName; }
            set { _UserName = value; }
        }

        private string _Password = "guest";

        /// <summary>
        /// Gets or sets the password to use for
        /// authentication with the message broker.
        /// The default is 'guest'
        /// </summary>
        public string Password
        {
            get { return _Password; }
            set { _Password = value; }
        }

        private uint _Port = 5672;

        /// <summary>
        /// Gets or sets the port to use
        /// for connections to the message broker (this is the broker's
        /// listening port).
        /// The default is '5672'.
        /// </summary>
        public uint Port
        {
            get { return _Port; }
            set { _Port = value; }
        }

        private string _Topic = "{0}";

        /// <summary>
        /// 	Gets or sets the routing key (aka. topic) with which
        /// 	to send messages. Defaults to {0}, which in the end is 'error' for log.Error("..."), and
        /// 	so on. An example could be setting this property to 'ApplicationType.MyApp.Web.{0}'.
        ///		The default is '{0}'.
        ///		
        ///     Superceeded by ExchangeProperties.Topic which is a bit more flexible.
        /// </summary>
        public string Topic
        {
            get { return _Topic; }
            set { _Topic = value; }
        }

        private string _HostName = "localhost";

        /// <summary>
        /// 	Gets or sets the host name of the broker to log to.
        /// </summary>
        /// <remarks>
        /// 	Default is 'localhost'
        /// </remarks>
        public string HostName
        {
            get { return _HostName; }
            set
            {
                if (value != null) _HostName = value;
            }
        }

        /// <summary>
        /// 	Gets or sets the exchange to bind the logger output to.
        /// </summary>
        /// <remarks>
        /// 	Default is 'app-logging'
        /// </remarks>
        public string Exchange
        {
            get { return this.ExchangeProperties.Name; }
            set
            {
                if (value != null) this.ExchangeProperties.Name = value;
            }
        }

        public string AppId
        {
            get { return this._MessageProperties.AppId; }
            set { this._MessageProperties.AppId = value; }
        }

        /// <summary>
        /// Gets or sets whether the logger should log extended data.
        /// Defaults to false.
        /// </summary>
        public bool ExtendedData
        {
            get { return this.MessageProperties.ExtendedData; }
            set { this.MessageProperties.ExtendedData = value; }
        }

        /// <summary>
        /// 	Gets or sets message properties used when messages are published.
        /// </summary>
        public ExchangeProperties ExchangeProperties
        {
            get { return this._ExchangeProperties; }
            set { this._ExchangeProperties = value ?? new ExchangeProperties(); }
        }

        /// <summary>
        /// 	Gets or sets message properties used when messages are published.
        /// </summary>
        public MessageProperties MessageProperties
        {
            get { return this._MessageProperties; }
            set { this._MessageProperties = value ?? new MessageProperties(); }
        }

        #endregion

        protected override void Append(LoggingEvent loggingEvent)
        {
            if (!_Bus.IsConnected)
                return;

            EasyNetQ.MessageProperties basicProperties = GetBasicProperties(loggingEvent);
            byte[] message = GetMessage(loggingEvent);
            string topic = GetTopic(loggingEvent);

            _Bus.Advanced.Publish(new Exchange(this.ExchangeProperties.Name), topic, false, basicProperties, message);
        }

        private EasyNetQ.MessageProperties GetBasicProperties(LoggingEvent loggingEvent)
        {
            var basicProperties = new EasyNetQ.MessageProperties();
            basicProperties.ContentEncoding = "utf8"; // because this is our _Encoding type
            basicProperties.ContentType = Format(loggingEvent);
            basicProperties.AppId = this.MessageProperties.AppId ?? loggingEvent.LoggerName;
            basicProperties.DeliveryMode = (byte)(this._MessageProperties.Persistent ? 2 : 1);

            this.InitMessagePriority(basicProperties, loggingEvent);

            basicProperties.Timestamp = Convert.ToInt64((loggingEvent.TimeStamp - _Epoch).TotalSeconds);

            // support Validated User-ID (see http://www.rabbitmq.com/extensions.html)
            basicProperties.UserId = UserName;

            if (this.MessageProperties.ExtendedData)
            {
                basicProperties.Headers = new Dictionary<string, object>();
                basicProperties.Headers["ClassName"] = loggingEvent.LocationInformation.ClassName;
                basicProperties.Headers["FileName"] = loggingEvent.LocationInformation.FileName;
                basicProperties.Headers["MethodName"] = loggingEvent.LocationInformation.MethodName;
                basicProperties.Headers["LineNumber"] = loggingEvent.LocationInformation.LineNumber;
            }

            return basicProperties;
        }

        private void InitMessagePriority(EasyNetQ.MessageProperties basicProperties, LoggingEvent loggingEvent)
        {
            // the priority must resolve to 0 - 9. Otherwise stick with the default.
            string sPriority = null;

            if (this.MessageProperties.Priority != null)
            {
                var sb = new StringBuilder();
                using (var sw = new StringWriter(sb))
                {
                    this.MessageProperties.Priority.Format(sw, loggingEvent);
                    sPriority = sw.ToString();
                }
            }

            int priority;
            if (int.TryParse(sPriority, out priority))
            {
                if ((priority >= 0) && (priority <= 9))
                {
                    basicProperties.Priority = (byte)priority;
                }
            }
        }

        private byte[] GetMessage(LoggingEvent loggingEvent)
        {
            var sb = new StringBuilder();
            using (var sr = new StringWriter(sb))
            {
                Layout.Format(sr, loggingEvent);

                if (Layout.IgnoresException && loggingEvent.ExceptionObject != null)
                    sr.Write(loggingEvent.GetExceptionString());

                return _Encoding.GetBytes(sr.ToString());
            }
        }

        protected virtual string Format(LoggingEvent loggingEvent)
        {
            return this.MessageProperties.ContentType.Format(loggingEvent);
        }

        private string GetTopic(LoggingEvent loggingEvent)
        {
            // format the topic, use the TopicLayout if it's set....
            string topic = null;
            if (this.MessageProperties.Topic != null)
            {
                var sb = new StringBuilder();
                using (var sw = new StringWriter(sb))
                {
                    this.MessageProperties.Topic.Format(sw, loggingEvent);
                    topic = sw.ToString();
                }
            }
            // ...and default back to the Topic format if TopicLayout is not set.
            if (string.IsNullOrEmpty(topic))
            {
                topic = string.Format(_Topic, loggingEvent.Level.Name);
            }

            return topic;
        }

        #region StartUp and ShutDown

        public override void ActivateOptions()
        {
            base.ActivateOptions();
            StartConnection();
        }

        private async void StartConnection()
        {
            try
            {
                // do this completely in async (unfortunatelly EasyNetQ v0.59.0.434 doesn't support the first connection in async)
                _Bus = await Task.Run(() =>
                {
                    var connection = GetConnection();
                    return RabbitHutch.CreateBus(connection, x => x.Register<IEasyNetQLogger>(s => new NullLogger()));
                }).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                ErrorHandler.Error("could not create the EasyNetQ bus instance", e);
            }

            try
            {
                var exchange = await _Bus.Advanced.ExchangeDeclareAsync(
                    this.ExchangeProperties.Name,
                    this.ExchangeProperties.ExchangeType,
                    durable: this.ExchangeProperties.Durable,
                    autoDelete: this.ExchangeProperties.AutoDelete).ConfigureAwait(false);

                foreach (var exchangeBinding in this.ExchangeProperties.Bindings)
                {
                    _Bus.Advanced.Bind(exchange, new Exchange(exchangeBinding.Destination), exchangeBinding.Topic);
                }

                this.Debug("apqp connection opened");
            }
            catch (Exception e)
            {
                ErrorHandler.Error("could not declare the exchange", e);
            }
        }

        private ConnectionConfiguration GetConnection()
        {
            var connection = new ConnectionConfiguration();

            connection.Port = (ushort)Port;
            connection.UserName = UserName;
            connection.Password = Password;
            connection.Product = AppId;
            connection.UseBackgroundThreads = true;

            var hosts = new List<HostConfiguration>();
            foreach (var hostName in HostName.Split(','))
            {
                var host = new HostConfiguration();
                host.Host = hostName;
                host.Port = (ushort)Port;
                if (Ssl.Enabled)
                {
                    host.Ssl.Enabled = true;
                    host.Ssl.ServerName = Ssl.ServerName;
                    host.Ssl.CertPath = Ssl.CertPath;
                    host.Ssl.CertPassphrase = Ssl.CertPassphrase;
                    host.Ssl.AcceptablePolicyErrors = Ssl.AcceptablePolicyErrors;
                    host.Ssl.CertificateSelectionCallback = Ssl.CertificateSelectionCallback;
                    host.Ssl.CertificateValidationCallback = Ssl.CertificateValidationCallback;
                    host.Ssl.Certs = Ssl.Certs;
                    host.Ssl.Version = Ssl.Version;
                }
                hosts.Add(host);
            }
            connection.Hosts = hosts;

            connection.Validate();

            return connection;
        }

        private SslOption _Ssl = new SslOption();

        protected SslOption Ssl
        {
            get { return _Ssl; }
            set { _Ssl = value; }
        }

        protected override void OnClose()
        {
            base.OnClose();

            ShutdownAmqp();
        }

        private void ShutdownAmqp()
        {
            try
            {
                if (_Bus != null)
                {
                    _Bus.Dispose();
                }
            }
            catch (Exception e)
            {
                ErrorHandler.Error("could not dispose EasyNetQ bus", e);
            }
        }

        #endregion

        protected virtual void Debug(string format, params object[] args)
        {
            Util.LogLog.Debug(typeof(RabbitMQAppender), string.Format(format, args));
        }
    }
}