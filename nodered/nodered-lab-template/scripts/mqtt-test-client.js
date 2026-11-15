/**
 * Companion MQTT test client for Lecture 2.2 (Node-RED Networking MQTT).
 * Connects to the local Mosquitto broker on mqtt://localhost:1883.
 *
 * Usage:
 *   node mqtt-test-client.js pub [/nodered/test]
 *   node mqtt-test-client.js sub [/nodered/test]
 *   node mqtt-test-client.js sensor
 */
const mqtt = require("mqtt");

const mode = process.argv[2] || "pub";
const topic = process.argv[3] || "/nodered/test";
const brokerUrl = "mqtt://localhost:1883";

const client = mqtt.connect(brokerUrl);

client.on("error", (err) => {
    console.error("MQTT connection error:", err.message);
    console.error("Assicurati che Mosquitto sia avviato (..\\start-mqtt.ps1)");
    client.end(true);
    process.exit(1);
});

client.on("connect", () => {
    console.log(`Connected to ${brokerUrl}`);

    if (mode === "sub") {
        client.subscribe(topic, { qos: 1 }, (err) => {
            if (err) {
                console.error("Subscribe error:", err);
                client.end();
                return;
            }
            console.log(`Subscribed to '${topic}' (Ctrl+C to exit)...`);
        });
        client.on("message", (t, message, packet) => {
            console.log(`[MQTT RECV] topic=${t} qos=${packet.qos} retain=${packet.retain} payload=${message.toString()}`);
        });
    } else if (mode === "sensor") {
        const sensorTopic = "/smartcity/milan";
        const payload = JSON.stringify({
            id: 10372898440,
            timestamp: new Date().toISOString(),
            sensordatavalues: [
                { id: 22999226740, value_type: "temperature", value: (15 + Math.random() * 10).toFixed(2) },
                { id: 22999226768, value_type: "humidity", value: (50 + Math.random() * 40).toFixed(2) }
            ],
            sensor: { id: 21666, pin: "7", sensor_type: { id: 9, name: "DHT22" } },
            location: { latitude: "45.486", longitude: "9.19", country: "IT" }
        });
        client.publish(sensorTopic, payload, { qos: 1 }, (err) => {
            if (err) console.error("Publish error:", err);
            else console.log(`Published Sensor.Community sample to '${sensorTopic}':\n${payload}`);
            client.end();
        });
    } else {
        const payload = JSON.stringify({ sender: "mqtt-test-client", timestamp: Date.now() });
        client.publish(topic, payload, { qos: 2 }, (err) => {
            if (err) console.error("Publish error:", err);
            else console.log(`Published to '${topic}' (QoS 2): ${payload}`);
            client.end();
        });
    }
});
