/**
 * Isolated Node-RED userDir for Lab 1 and Lab 2 flows (lab/lab1/flows.json & lab/lab2/flows.json).
 * Keeps lab context, SQLite databases and runtime metadata separate from nodered-exam and nodered-practice.
 */
module.exports = {
    flowFilePretty: true,
    uiPort: process.env.PORT || 1880,
    contextStorage: {
        default: {
            module: "memory"
        },
        file: {
            module: "localfilesystem"
        }
    },
    functionExternalModules: true,
    functionGlobalContext: {},
    logging: {
        console: {
            level: "info",
            metrics: false,
            audit: false
        }
    },
    editorTheme: {
        projects: {
            enabled: false
        }
    }
};
