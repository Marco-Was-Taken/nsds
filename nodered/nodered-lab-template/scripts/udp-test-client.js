/**
 * Companion UDP test client for Lecture 2.1 (Node-RED Networking UDP).
 * Usage:
 *   node udp-test-client.js send [port=7777]
 *   node udp-test-client.js listen [port=7777]
 */
const dgram = require("dgram");

const mode = process.argv[2] || "send";
const port = parseInt(process.argv[3] || "7777", 10);
const host = "127.0.0.1";

if (mode === "listen") {
    const server = dgram.createSocket("udp4");
    server.on("message", (msg, rinfo) => {
        const text = msg.toString("utf8");
        console.log(`[UDP RECV from ${rinfo.address}:${rinfo.port}] raw=${text}`);
        try {
            console.log("  parsed JSON:", JSON.parse(text));
        } catch (_) {
            // plain text payload
        }
    });
    server.bind(port, host, () => {
        console.log(`Listening for UDP datagrams on ${host}:${port} (Ctrl+C to exit)...`);
    });
} else {
    const client = dgram.createSocket("udp4");
    const payload = JSON.stringify({
        greeting: "Hello from Node.js!",
        timestamp: Date.now()
    });
    const buffer = Buffer.from(payload, "utf8");
    client.send(buffer, port, host, (err) => {
        if (err) {
            console.error("UDP send error:", err);
            process.exitCode = 1;
        } else {
            console.log(`Sent UDP JSON to ${host}:${port} -> ${payload}`);
        }
        client.close();
    });
}
