/**
 * Node-RED settings for NSDS Exam / Lab workspace (nodered-exam).
 * Mirrors Node-RED defaults while enabling:
 *  - Clean local userDir (flows.json & context files stay inside nodered-exam/)
 *  - Optional persistent filesystem context store ("file") alongside in-memory ("default")
 */
module.exports = {
    // Default flow file inside userDir
    flowFile: "flows.json",
    flowFilePretty: true,

    // Web editor port (http://localhost:1880)
    uiPort: process.env.PORT || 1880,

    // Context storage backends (Lecture 1.3 - Data Sharing):
    //   - default (in-memory): fast, re-initialized on restart (and cleared on full deploy)
    //   - file (localfilesystem): persisted on disk in nodered-exam/context/ across restarts/deploys
    // Usage in Function nodes:
    //   context.get("count")          -> uses "default" (memory)
    //   context.get("count", "file")  -> uses "file" (persistent)
    //   flow.set("total", 42, "file") -> persistent flow context
    contextStorage: {
        default: {
            module: "memory"
        },
        file: {
            module: "localfilesystem"
        }
    },

    // Allow Function nodes to load external npm modules dynamically if needed
    functionExternalModules: true,

    // Expose useful Node.js globals to Function nodes via global.get(...) if desired
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
