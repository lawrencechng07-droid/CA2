using System.Text.Json;
using MQTTnet;
using MQTTnet.Protocol;

var builder = WebApplication.CreateBuilder(args);

// ======================================================
// 1. REGISTER SERVICES
// ======================================================

builder.Services.AddSingleton<DashboardState>();
builder.Services.AddHostedService<MqttWorker>();

var app = builder.Build();

// ======================================================
// 2. API ENDPOINTS
// ======================================================

// Return current PLC data to the dashboard.
app.MapGet("/api/status", (DashboardState state) =>
{
    return Results.Json(state.GetSnapshot());
});

// Display the dashboard in a browser.
app.MapGet("/", () =>
{
    return Results.Content(
        DashboardHtml.Page,
        "text/html");
});

app.Run();


// ======================================================
// 3. DASHBOARD STATE
// ======================================================

public sealed class DashboardState
{
    private readonly object _lock = new();

    private bool _connected;
    private bool _pushButton;
    private bool _proximitySensor;

    private bool _greenLight;
    private bool _yellowLight;
    private bool _redLight;

    private double? _voltage;

    private string _lastMessage =
        "Waiting for PLC data...";

    private DateTime? _lastUpdate;

    public void SetConnection(bool connected)
    {
        lock (_lock)
        {
            _connected = connected;
        }
    }

    public void SetMessage(string message)
    {
        lock (_lock)
        {
            _lastMessage = message;
        }
    }

    // Process incoming PLC JSON data.
    public void Update(JsonElement json)
    {
        lock (_lock)
        {
            _pushButton =
                ReadBool(json, "push_button");

            _proximitySensor =
                ReadBool(json, "sensor_pnp");

            _greenLight =
                ReadBool(json, "green_light");

            _yellowLight =
                ReadBool(json, "yellow_light");

            _redLight =
                ReadBool(json, "red_light");

            _voltage =
                ReadNumber(json, "voltage");

            _lastUpdate = DateTime.UtcNow;

            _lastMessage = "PLC data received";
        }
    }

    public object GetSnapshot()
    {
        lock (_lock)
        {
            return new
            {
                connected = _connected,

                pushButton = _pushButton,

                proximitySensor = _proximitySensor,

                greenLight = _greenLight,

                yellowLight = _yellowLight,

                redLight = _redLight,

                voltage = _voltage,

                lastMessage = _lastMessage,

                lastUpdate = _lastUpdate
            };
        }
    }

    // Read a Boolean or numeric 0/1 from JSON.
    private static bool ReadBool(
        JsonElement json,
        string name)
    {
        if (!json.TryGetProperty(name, out var value))
        {
            return false;
        }

        if (value.ValueKind == JsonValueKind.True)
        {
            return true;
        }

        if (value.ValueKind == JsonValueKind.False ||
            value.ValueKind == JsonValueKind.Null)
        {
            return false;
        }

        if (value.ValueKind == JsonValueKind.Number)
        {
            return value.TryGetDouble(out var number)
                   && number != 0;
        }

        if (value.ValueKind == JsonValueKind.String)
        {
            string? text = value.GetString();

            return text == "1" ||
                   string.Equals(
                       text,
                       "true",
                       StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    // Read a numeric value from JSON.
    private static double? ReadNumber(
        JsonElement json,
        string name)
    {
        if (!json.TryGetProperty(name, out var value))
        {
            return null;
        }

        if (value.ValueKind == JsonValueKind.Number &&
            value.TryGetDouble(out var number))
        {
            return number;
        }

        if (value.ValueKind == JsonValueKind.String &&
            double.TryParse(
                value.GetString(),
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture,
                out number))
        {
            return number;
        }

        return null;
    }
}


// ======================================================
// 4. MQTT WORKER
// ======================================================

public sealed class MqttWorker : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly DashboardState _state;
    private readonly ILogger<MqttWorker> _logger;

    public MqttWorker(
        IConfiguration configuration,
        DashboardState state,
        ILogger<MqttWorker> logger)
    {
        _configuration = configuration;
        _state = state;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        string host =
            _configuration["Mqtt:Host"]
            ?? "192.168.101.125";

        int port =
            _configuration.GetValue<int>(
                "Mqtt:Port",
                1883);

        string topic =
            _configuration["Mqtt:Topic"]
            ?? "beaver-iot/mqtt@default/mqtt-device/PLC_111";

        string username =
            _configuration["Mqtt:Username"]
            ?? "mqtt@default";

        string password =
            _configuration["Mqtt:Password"]
            ?? "";

        var factory = new MqttClientFactory();

        using var client = factory.CreateMqttClient();

        // --------------------------------------------------
        // RECEIVE MQTT MESSAGES
        // --------------------------------------------------

        client.ApplicationMessageReceivedAsync += e =>
        {
            try
            {
                string payload =
                    e.ApplicationMessage.ConvertPayloadToString();

                using var document =
                    JsonDocument.Parse(payload);

                _state.Update(document.RootElement);

                _logger.LogInformation(
                    "PLC data received: {Payload}",
                    payload);
            }
            catch (Exception ex)
            {
                _state.SetMessage(
                    "Error processing PLC message");

                _logger.LogWarning(
                    ex,
                    "Unable to process MQTT message");
            }

            return Task.CompletedTask;
        };

        // --------------------------------------------------
        // CONNECT AND SUBSCRIBE
        // --------------------------------------------------

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
                                "PLC-Dashboard-" +
                                Guid.NewGuid().ToString("N")[..8]);

                    if (!string.IsNullOrWhiteSpace(username))
                    {
                        optionsBuilder.WithCredentials(
                            username,
                            password);
                    }

                    _logger.LogInformation(
                        "Connecting to Beaver IoT at {Host}:{Port}",
                        host,
                        port);

                    var result = await client.ConnectAsync(
                        optionsBuilder.Build(),
                        stoppingToken);

                    if (!client.IsConnected)
                    {
                        _state.SetConnection(false);

                        throw new InvalidOperationException(
                            $"MQTT connection unsuccessful: {result.ResultCode}");
                    }

                    _logger.LogInformation(
                        "MQTT connected: {ResultCode}",
                        result.ResultCode);

                    var subscribeOptions =
                        new MqttClientSubscribeOptionsBuilder()
                            .WithTopicFilter(filter => filter
                                .WithTopic(topic)
                                .WithQualityOfServiceLevel(
                                    MqttQualityOfServiceLevel.AtLeastOnce))
                            .Build();

                    await client.SubscribeAsync(
                        subscribeOptions,
                        stoppingToken);

                    _state.SetConnection(true);

                    _state.SetMessage(
                        "Connected; waiting for PLC data");

                    _logger.LogInformation(
                        "Subscribed to topic: {Topic}",
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
                _state.SetConnection(false);

                _state.SetMessage(
                    $"MQTT error: {ex.Message}");

                _logger.LogError(
                    ex,
                    "MQTT connection or subscription failed. Retrying in 5 seconds.");

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

        _state.SetConnection(false);
    }
}


// ======================================================
// 5. HTML DASHBOARD
// ======================================================

public static class DashboardHtml
{
    public const string Page = """
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">

    <meta name="viewport"
          content="width=device-width, initial-scale=1">

    <title>WAGO PLC IIoT Dashboard</title>

    <style>
        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #f1f5f9;
            color: #172033;
        }

        header {
            background: #102a43;
            color: white;
            padding: 25px 15px;
            text-align: center;
        }

        header h1 {
            margin: 0 0 8px;
        }

        header p {
            margin: 0;
            color: #d9e8f5;
        }

        main {
            max-width: 1100px;
            margin: 25px auto;
            padding: 0 16px;
        }

        .grid {
            display: grid;
            grid-template-columns:
                repeat(auto-fit, minmax(220px, 1fr));
            gap: 18px;
        }

        .card {
            background: white;
            border-radius: 12px;
            padding: 22px;
            box-shadow: 0 2px 8px #00000012;
        }

        .card h2 {
            margin-top: 0;
            font-size: 17px;
            color: #526477;
        }

        .value {
            font-size: 25px;
            font-weight: bold;
            margin-top: 16px;
        }

        .lamp {
            display: inline-block;
            width: 24px;
            height: 24px;
            border-radius: 50%;
            background: #cbd5e1;
            border: 2px solid #94a3b8;
            vertical-align: middle;
            margin-right: 8px;
        }

        .on.green {
            background: #22c55e;
            border-color: #15803d;
            box-shadow: 0 0 8px #22c55e80;
        }

        .on.yellow {
            background: #facc15;
            border-color: #ca8a04;
            box-shadow: 0 0 8px #facc1580;
        }

        .on.red {
            background: #ef4444;
            border-color: #b91c1c;
            box-shadow: 0 0 8px #ef444480;
        }

        .on.blue {
            background: #3b82f6;
            border-color: #1d4ed8;
        }

        .status {
            display: inline-block;
            padding: 8px 13px;
            border-radius: 20px;
            font-weight: bold;
            background: #e2e8f0;
        }

        .online {
            background: #dcfce7;
            color: #166534;
        }

        .offline {
            background: #fee2e2;
            color: #991b1b;
        }

        .footer {
            margin-top: 22px;
            color: #64748b;
            font-size: 13px;
            text-align: center;
        }

        @media (max-width: 600px) {
            header h1 {
                font-size: 23px;
            }

            .value {
                font-size: 22px;
            }
        }
    </style>
</head>

<body>

<header>
    <h1>WAGO PLC IIoT Monitoring</h1>
    <p>Beaver IoT | Device: PLC_111</p>
</header>

<main>

    <div class="grid">

        <section class="card">
            <h2>MQTT Connection</h2>

            <span id="connection"
                  class="status offline">
                Disconnected
            </span>

            <p id="message">
                Waiting for PLC data...
            </p>
        </section>

        <section class="card">
            <h2>Analog Input</h2>

            <div class="value">
                <span id="voltage">--</span> V
            </div>

            <p>Latest reported voltage</p>
        </section>

        <section class="card">
            <h2>Push Button</h2>

            <div class="value">
                <span id="buttonLamp"
                      class="lamp"></span>

                <span id="buttonText">OFF</span>
            </div>
        </section>

        <section class="card">
            <h2>Proximity Sensor</h2>

            <div class="value">
                <span id="sensorLamp"
                      class="lamp"></span>

                <span id="sensorText">OFF</span>
            </div>
        </section>

        <section class="card">
            <h2>Green Indicator</h2>

            <div class="value">
                <span id="greenLamp"
                      class="lamp"></span>

                <span id="greenText">OFF</span>
            </div>

            <p>Push button only</p>
        </section>

        <section class="card">
            <h2>Yellow Indicator</h2>

            <div class="value">
                <span id="yellowLamp"
                      class="lamp"></span>

                <span id="yellowText">OFF</span>
            </div>

            <p>Proximity sensor only</p>
        </section>

        <section class="card">
            <h2>Red Indicator</h2>

            <div class="value">
                <span id="redLamp"
                      class="lamp"></span>

                <span id="redText">OFF</span>
            </div>

            <p>Both inputs active</p>
        </section>

        <section class="card">
            <h2>Operating Condition</h2>

            <div class="value"
                 id="operatingStatus">
                WAITING
            </div>

            <p>Current input condition</p>
        </section>

        <section class="card">
            <h2>Last PLC Update</h2>

            <div class="value"
                 style="font-size:20px"
                 id="lastUpdate">
                --
            </div>

            <p>Latest received message</p>
        </section>

    </div>

    <p class="footer">
        The WAGO PLC controls the physical indicator lights.
        This dashboard displays the states received over MQTT.
    </p>

</main>

<script>
function setLamp(id, active, color) {
    const lamp = document.getElementById(id);

    lamp.className =
        "lamp" + (active ? " on " + color : "");
}

function setState(textId, lampId, active, color) {
    document.getElementById(textId).textContent =
        active ? "ON" : "OFF";

    setLamp(lampId, active, color);
}

async function refreshDashboard() {
    try {
        const response = await fetch("/api/status", {
            cache: "no-store"
        });

        if (!response.ok) {
            throw new Error("Status API request failed");
        }

        const d = await response.json();

        const connection =
            document.getElementById("connection");

        connection.textContent =
            d.connected ? "Connected" : "Disconnected";

        connection.className =
            "status " +
            (d.connected ? "online" : "offline");

        document.getElementById("message").textContent =
            d.lastMessage ?? "";

        document.getElementById("voltage").textContent =
            d.voltage == null
                ? "--"
                : Number(d.voltage).toFixed(2);

        setState(
            "buttonText",
            "buttonLamp",
            d.pushButton,
            "blue"
        );

        setState(
            "sensorText",
            "sensorLamp",
            d.proximitySensor,
            "blue"
        );

        setState(
            "greenText",
            "greenLamp",
            d.greenLight,
            "green"
        );

        setState(
            "yellowText",
            "yellowLamp",
            d.yellowLight,
            "yellow"
        );

        setState(
            "redText",
            "redLamp",
            d.redLight,
            "red"
        );

        let status = "SYSTEM IDLE";

        if (d.pushButton && d.proximitySensor) {
            status = "BOTH INPUTS DETECTED";
        }
        else if (d.pushButton) {
            status = "PUSH BUTTON ACTIVATED";
        }
        else if (d.proximitySensor) {
            status = "OBJECT DETECTED";
        }

        document.getElementById("operatingStatus")
            .textContent = status;

        document.getElementById("lastUpdate")
            .textContent = d.lastUpdate
                ? new Date(d.lastUpdate).toLocaleTimeString()
                : "--";
    }
    catch (error) {
        const connection =
            document.getElementById("connection");

        connection.textContent = "Dashboard Error";
        connection.className = "status offline";
    }
}

refreshDashboard();

setInterval(refreshDashboard, 1000);
</script>

</body>
</html>
""";
}