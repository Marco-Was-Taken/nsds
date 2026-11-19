/**
 * Node-RED settings for NSDS Practice / Extra Exercises workspace (nodered-practice).
 * Isolated userDir on port 1881 (can run simultaneously with nodered-exam on port 1880):
 *  - Clean local userDir (flows.json & context files stay inside nodered-practice/)
 *  - Persistent filesystem context store ("file") alongside in-memory ("default")
 */
module.exports = {
    flowFile: "flows.json",
    flowFilePretty: true,

    // Dedicated web editor port (http://localhost:1881) to avoid conflicts with Exam (1880)
    uiPort: process.env.PORT || 1881,

    contextStorage: {
        default: {
            module: "memory"
        },
        file: {
            module: "localfilesystem"
        }
    },

    functionExternalModules: true,

    functionGlobalContext: {
        // os: require("os")
    },

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
