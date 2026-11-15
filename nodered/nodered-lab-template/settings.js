module.exports = {
    flowFile: "flows/all_templates_combined.json",
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
