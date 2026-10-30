using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using MQTTnet;
using MQTTnet.Protocol;

public sealed class MqttBackgroundService : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly MqttDataStore _dataStore;
    private readonly ILogger<MqttBackgroundService> _logger;

    public MqttBackgroundService(
        IConfiguration configuration,
        MqttDataStore dataStore,
        ILogger<MqttBackgroundService> logger)
    {
        _configuration = configuration;
        _dataStore = dataStore;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        string host =
      _configuration["Mqtt:Host"] ?? "192.168.101.125";

        int port =
            _configuration.GetValue<int>("Mqtt:Port", 1883);

        string topic =
            _configuration["Mqtt:Topic"]
            ?? "beaver-iot/mqtt@default/mqtt-device/PLC_111";

        string username =
            _configuration["Mqtt:Username"] ?? "";

        string password =
            _configuration["Mqtt:Password"] ?? "";

        var factory = new MqttClientFactory();

        using var client = factory.CreateMqttClient();

        // Process messages received from the WAGO PLC.
        client.ApplicationMessageReceivedAsync += async e =>
        {
            try
            {
                // MQTTnet exposes the payload as ReadOnlySequence<byte>.
                // ToArray() converts it into a byte array.
                byte[] payloadBytes =
                    System.Text.Encoding.UTF8.GetBytes(
                        e.ApplicationMessage.ConvertPayloadToString());

                string payload =
                    Encoding.UTF8.GetString(payloadBytes);

                _logger.LogInformation(
                    "MQTT message received on {Topic}: {Payload}",
                    e.ApplicationMessage.Topic,
                    payload);

                using JsonDocument document =
                    JsonDocument.Parse(payload);

                _dataStore.Update(document.RootElement);
            }
            catch (JsonException ex)
            {
                _dataStore.SetMessage("Received invalid JSON");

                _logger.LogWarning(
                    ex,
                    "Unable to parse incoming MQTT JSON");
            }
            catch (Exception ex)
            {
                _dataStore.SetMessage("Error processing PLC data");

                _logger.LogError(
                    ex,
                    "Error processing incoming MQTT message");
            }

            await Task.CompletedTask;
        };

        // Connect and reconnect until the application stops.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!client.IsConnected)
                {
                    var optionsBuilder =
                        new MqttClientOptionsBuilder()
                            .WithTcpServer(host, port)
                            .WithClientId(
                                "PW7-MudBlazor-" +
                                Guid.NewGuid().ToString("N")[..8]);

                    if (!string.IsNullOrWhiteSpace(username))
                    {
                        optionsBuilder.WithCredentials(
                            username,
                            password);
                    }

                    var options = optionsBuilder.Build();

                    _logger.LogInformation(
                        "Connecting to MQTT broker at {Host}:{Port}",
                        host,
                        port);

                    await client.ConnectAsync(
                        options,
                        stoppingToken);

                    var topicFilter =
                        new MqttTopicFilterBuilder()
                            .WithTopic(topic)
                            .WithQualityOfServiceLevel(
                                MqttQualityOfServiceLevel.AtLeastOnce)
                            .Build();

                    var subscription =
                        new MqttClientSubscribeOptionsBuilder()
                            .WithTopicFilter(topicFilter)
                            .Build();

                    await client.SubscribeAsync(
                        subscription,
                        stoppingToken);

                    _dataStore.SetConnection(true);

                    _dataStore.SetMessage(
                        "Connected to MQTT; waiting for PLC data");

                    _logger.LogInformation(
                        "Successfully subscribed to topic {Topic}",
                        topic);
                }

                await Task.Delay(
                    TimeSpan.FromSeconds(1),
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _dataStore.SetConnection(false);

                _dataStore.SetMessage(
                    "MQTT connection failed; retrying");

                _logger.LogError(
                    ex,
                    "MQTT connection or subscription failed");

                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(5),
                        stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        _dataStore.SetConnection(false);

        _logger.LogInformation(
            "MQTT background service stopped");
    }
}
