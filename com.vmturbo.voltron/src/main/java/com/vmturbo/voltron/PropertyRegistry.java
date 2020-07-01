package com.vmturbo.voltron;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.env.PropertiesPropertySource;

import com.vmturbo.components.common.config.ConfigMapPropertiesReader;

/**
 * Properties (both global and component-specific).
 */
class PropertyRegistry {
    private static final Logger logger = LogManager.getLogger();

    private final String dataPath;

    private final String namespace;

    private final VoltronConfiguration voltronConfiguration;

    PropertyRegistry(String namespace, String dataPath, VoltronConfiguration voltronConfiguration) {
        this.namespace = namespace;
        this.dataPath = dataPath;
        this.voltronConfiguration = voltronConfiguration;
    }

    @Nonnull
    private Properties getGlobalOverrides() {
        Properties props = new Properties();
        // Override certain global properties.
        props.put("kafkaServers", "localhost:9093");

        props.put("arangoDBPassword", "root");

        props.put("actionOrchestratorHost", "localhost");
        props.put("authHost", "localhost");
        props.put("clusterMgrHost", "localhost");
        props.put("costHost", "localhost");
        props.put("dbHost", "localhost");
        props.put("groupHost", "localhost");
        props.put("historyHost", "localhost");
        props.put("marketHost", "localhost");
        props.put("planOrchestratorHost", "localhost");
        props.put("reportingHost", "localhost");
        props.put("repositoryHost", "localhost");
        props.put("topologyProcessorHost", "localhost");
        props.put("apiHost", "localhost");
        props.put("grafanaHost", "localhost");

        props.put("postgresPort", "5432");

        props.put("authDbPassword", "vmturbo");
        props.put("clustermgrDbPassword", "vmturbo");
        props.put("actionDbPassword", "vmturbo");
        props.put("costDbPassword", "vmturbo");
        props.put("groupComponentDbPassword", "vmturbo");
        props.put("historyDbPassword", "vmturbo");
        props.put("planDbPassword", "vmturbo");
        props.put("topologyProcessorDbPassword", "vmturbo");

        props.put("serverHttpPort", voltronConfiguration.getServerHttpPort());
        props.put("serverGrpcPort", voltronConfiguration.getServerGrpcPort());

        // Mediation Component Common
        props.put("serverAddress", getServerAddress());
        props.put("ux-path", getUxPath());

        props.put("clusterMgrRoute", Component.CLUSTERMGR.getShortName());
        props.put("authRoute", Component.AUTH.getPathPrefix());
        props.put("consul_host", "localhost");
        props.put("consul_port", "8500");

        props.put("grafanaHost", "localhost");

        props.put("topologyProcessorRoute", Component.TOPOLOGY_PROCESSOR.getShortName());
        if (!namespace.isEmpty()) {
            // Consul
            props.setProperty("enableConsulNamespace", "true");
            props.setProperty("consulNamespace", namespace);

            // Kafka
            props.setProperty("kafkaNamespace", namespace);

            // Arango
            props.setProperty("arangoDBNamespace", namespace);

            // SQL schemas are on a per-component basis, so we don't do them here.
        }
        return props;
    }

    private String getServerAddress() {
        return "ws://localhost:" + voltronConfiguration.getServerHttpPort() + "/remoteMediation";
    }

    private String getUxPath() {
        String uxPath = voltronConfiguration.getUxPath();
        File f = new File(uxPath);
        if (!f.exists() || !f.isDirectory()) {
            throw new IllegalArgumentException("UX path must point to a valid directory to "
                    + "serve the UI from (e.g. ux-app/.tmp");
        }
        return uxPath;
    }

    @Nonnull
    PropertiesPropertySource getComponentProperties(final String shortName,
                                                    final String topFolder,
                                                    final String restRoute) throws IOException {
        // Start with the config map properties, which take lowest priority.
        Properties props = ConfigMapPropertiesReader.readConfigMap(shortName, "file:" + Voltron.getAbsolutePath("com.vmturbo.voltron/src/main/resources/config/configmap.yaml"));
        // Apply global overrides.
        props.putAll(getGlobalOverrides());

        // Additional overrides.
        props.put("migrationLocation", Voltron.migrationLocation(topFolder));
        props.put("com.vmturbo.kvdir", Paths.get(dataPath, namespace, "kvdir", topFolder).toString());
        props.put("discoveryResponsesCachePath", Paths.get(dataPath, namespace, "cached_responses", topFolder).toString());
        props.put("component_type", shortName);
        props.put("instance_route", restRoute);
        props.put("instance_id", shortName + "-1");
        if (!namespace.isEmpty()) {
            // If we are setting a namespace, override the SQL schema name with a prefixed name.
            Component.forShortName(shortName)
                .flatMap(Component::getDbSchema)
                .ifPresent(schema -> {
                    props.put("dbSchemaName", namespace + "_" + schema.getName());
                });
        }

        // User-specified overrides go last, so they override anything previously set.
        Component.forShortName(shortName).ifPresent(component -> {
            // Add any additional overrides for this component.
            props.putAll(voltronConfiguration.getComponentOverrides(component));
        });
        return new PropertiesPropertySource(topFolder, props);
    }
}
