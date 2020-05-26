package com.vmturbo.voltron;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.Protocol;
import com.ecwid.consul.v1.ConsulClient;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.components.api.SetOnce;
import com.vmturbo.kvstore.ConsulKeyValueStore;
import com.vmturbo.repository.RepositoryComponentConfig;
import com.vmturbo.sql.utils.TestDbConfiguration;
import com.vmturbo.voltron.Voltron.VoltronContext;

/**
 * Zarkon is Voltron's greatest enemy (according to Wikipedia). It's responsible for destroying
 * Voltron's data.
 */
class Zarkon implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private final AtomicBoolean demolished = new AtomicBoolean(false);

    private final String namespace;
    private final VoltronConfiguration config;
    private final SetOnce<VoltronContext> contextOpt;

    Zarkon(String namespace, VoltronConfiguration config, SetOnce<VoltronContext> context) {
        this.namespace = namespace;
        this.config = config;
        this.contextOpt = context;
    }

    private void demolish() {
        if (demolished.compareAndSet(false, true)) {
            if (!contextOpt.getValue().isPresent()) {
                logger.info("Skipping demolition because of empty context.");
                return;
            }

            // The context is probably not initialized so we can't get bean objects from it,
            // but we can still get the property values.
            VoltronContext context = contextOpt.getValue().get();

            AnnotationConfigWebApplicationContext repoContext =
                    context.getComponents().get(Component.REPOSITORY);
            if (repoContext != null) {
                ConfigurableEnvironment repoEnv = repoContext.getEnvironment();
                // Create an arango driver, and drop the database.
                try {
                    ArangoDB arangoDB = new ArangoDB.Builder().host(repoEnv.getProperty("REPOSITORY_ARANGODB_HOST", "localhost"),
                            Integer.parseInt(repoEnv.getProperty("arangoDBPort", "8529")))
                            .password(repoEnv.getProperty("arangoDBPassword", "root"))
                            .user(repoEnv.getProperty("arangoDBUsername", "root"))
                            .maxConnections(5)
                            .useProtocol(Protocol.HTTP_VPACK)
                            .build();
                    ArangoDatabase db = arangoDB.db(RepositoryComponentConfig.DATABASE_NAME_PREFIX + namespace);
                    final boolean success = db.drop();
                    if (success) {
                        logger.info("Dropped arango database: {}", db.name());
                    } else {
                        logger.warn(
                                "Database {} was not dropped successfully. May not have existed.",
                                db.name());
                    }
                } catch (Exception e) {
                    logger.error("Failed to demolish arango database.", e);
                }
            }

            // Clean up SQL.
            context.getComponents().forEach((component, uninitializedContext) -> {
                if (component.getDbSchema().isPresent()) {
                    // Configure the appropriate data source, and then delete all the data.
                    try {
                        TestDbConfiguration testDbConfiguration =
                                new TestDbConfiguration(component.getDbSchema().get().getName(),
                                        uninitializedContext.getEnvironment().getProperty("dbSchemaName"));
                        testDbConfiguration.getFlyway().clean();
                        logger.info("Finished cleaning database for component {}", component);
                    } catch (Exception e) {
                        logger.error("Failed to clean up database for component {}.", component, e);
                    }
                }
            });

            // Clean up consul.
            ConfigurableEnvironment env = context.getRootContext().getEnvironment();
            try {
                ConsulClient consulClient =
                        new ConsulClient(env.getProperty("consul_host", "localhost"), Integer.parseInt(env.getProperty("consul_port", "8500")));
                String namespacePrefix = ConsulKeyValueStore.constructNamespacePrefix(env.getProperty("consulNamespace", namespace),
                        Boolean.parseBoolean(env.getProperty("enableConsulNamespace", "true")));
                consulClient.deleteKVValues(namespacePrefix);
                logger.info("Finished cleaning up consul in namespace {}", namespace);
            } catch (Exception e) {
                logger.error("Failed to clean up consul data.", e);
            }

            // Delete the local data directory.
            final Path dataPath = Paths.get(config.getDataPath());
            try {
                FileUtils.deleteDirectory(dataPath.toFile());
                logger.info("Data directory {} deleted.", dataPath.toString());
            } catch (IOException e) {
                logger.warn("Failed to delete data directory {}", dataPath.toString(), e);
            }
        }
    }

    @Override
    public void run() {
        if (config.cleanSlate()) {
            demolish();
        }
    }
}
