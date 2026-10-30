using System;
using System.Text.Json;

public sealed class MqttDataStore
{
    private readonly object _lock = new();

    private bool _connected;
    private bool _pushButton;
    private bool _proximitySensor;

    private bool _greenLight;
    private bool _yellowLight;
    private bool _redLight;

    private double? _voltage;

    private string _lastMessage = "Waiting for PLC data";
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

    public void Update(JsonElement json)
    {
        lock (_lock)
        {
            // Read input states.
            if (TryGetProperty(json, out var value,
                    "push_button", "pushButton"))
            {
                _pushButton = ReadBoolean(value);
            }

            if (TryGetProperty(json, out value,
                    "proximity_sensor", "proximitySensor",
                    "sensor_pnp", "sensor_pnp"))
            {
                _proximitySensor = ReadBoolean(value);
            }

            // Read indicator light states.
            if (TryGetProperty(json, out value,
                    "green_light", "greenLight"))
            {
                _greenLight = ReadBoolean(value);
            }

            if (TryGetProperty(json, out value,
                    "yellow_light", "yellowLight"))
            {
                _yellowLight = ReadBoolean(value);
            }

            if (TryGetProperty(json, out value,
                    "red_light", "redLight"))
            {
                _redLight = ReadBoolean(value);
            }

            // Read voltage or analog value.
            if (TryGetProperty(json, out value,
                    "voltage", "Voltage", "temperature"))
            {
                if (value.ValueKind == JsonValueKind.Number &&
                    value.TryGetDouble(out var number))
                {
                    _voltage = number;
                }
                else if (value.ValueKind == JsonValueKind.String &&
                         double.TryParse(
                             value.GetString(),
                             System.Globalization.NumberStyles.Float,
                             System.Globalization.CultureInfo.InvariantCulture,
                             out number))
                {
                    _voltage = number;
                }
            }

            _lastUpdate = DateTime.UtcNow;
            _lastMessage = "PLC data received successfully";
        }
    }

    public DashboardSnapshot GetSnapshot()
    {
        lock (_lock)
        {
            return new DashboardSnapshot
            {
                Connected = _connected,
                PushButton = _pushButton,
                ProximitySensor = _proximitySensor,
                GreenLight = _greenLight,
                YellowLight = _yellowLight,
                RedLight = _redLight,
                Voltage = _voltage,
                LastMessage = _lastMessage,
                LastUpdate = _lastUpdate
            };
        }
    }

    private static bool TryGetProperty(
        JsonElement json,
        out JsonElement value,
        params string[] names)
    {
        foreach (var property in json.EnumerateObject())
        {
            foreach (var name in names)
            {
                if (string.Equals(
                    property.Name,
                    name,
                    StringComparison.OrdinalIgnoreCase))
                {
                    value = property.Value;
                    return true;
                }
            }
        }

        value = default;
        return false;
    }

    private static bool ReadBoolean(JsonElement value)
    {
        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,

            JsonValueKind.Number =>
                value.TryGetInt32(out var number) && number != 0,

            JsonValueKind.String =>
                string.Equals(
                    value.GetString(), "1",
                    StringComparison.OrdinalIgnoreCase)
                || string.Equals(
                    value.GetString(), "true",
                    StringComparison.OrdinalIgnoreCase)
                || string.Equals(
                    value.GetString(), "ON",
                    StringComparison.OrdinalIgnoreCase)
                || string.Equals(
                    value.GetString(), "DETECTED",
                    StringComparison.OrdinalIgnoreCase),

            _ => false
        };
    }
}

public sealed class DashboardSnapshot
{
    public bool Connected { get; set; }

    public bool PushButton { get; set; }

    public bool ProximitySensor { get; set; }

    public bool GreenLight { get; set; }

    public bool YellowLight { get; set; }

    public bool RedLight { get; set; }

    public double? Voltage { get; set; }

    public string LastMessage { get; set; } = "";

    public DateTime? LastUpdate { get; set; }
}