package com.vmturbo.voltron;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessage;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceBlockingStub;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.components.api.localbus.LocalBus;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.external.api.TurboApiClient;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.securekvstore.VaultKeyValueStore;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.voltron.Voltron.VoltronContext;

/**
 * Contains all information about a started Voltron instance, and methods to manage and interact
 * with it programatically.
 */
public class VoltronsContainer {
    private static final Logger logger = LogManager.getLogger();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final VoltronContext context;
    private final VoltronConfiguration config;
    private final Zarkon demolitioner;

    VoltronsContainer(VoltronContext context,
                      final VoltronConfiguration config,
                      Zarkon demolitioner) {
        this.context = context;
        this.config = config;
        this.demolitioner = demolitioner;
    }

    /**
     * Clean data for a set of components. Note - this doesn't clean their in-memory state, so
     * might not be that useful.
     *
     * <p/>TODO - we can also (optionally) refresh contexts to clear in-memory data.
     *
     * @param components The components to clean data for.
     */
    public void clean(@Nonnull final Set<Component> components) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Component, AnnotationConfigWebApplicationContext> componentsToClean =
            context.getComponents().entrySet().stream()
                .filter(entry -> components.contains(entry.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        // Arango first - least dependencies.
        Optional.ofNullable(componentsToClean.get(Component.REPOSITORY))
                .ifPresent(repoContext -> cleanArango(Component.REPOSITORY.getShortName(), repoContext));
        componentsToClean.forEach(this::cleanSQL);
        // Consul/KV store last - lightweight data, potentially stores passwords.
        componentsToClean.forEach((component, context) -> {
            cleanConsul(component.getShortName(), context);
        });

        logger.info("Cleanup took {} seconds.", stopwatch.elapsed(TimeUnit.SECONDS));
    }

    /**
     * Shut down and, if necessary, destroy all the data.
     */
    public void demolish() {
        if (shutdown.compareAndSet(false, true)) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                context.getRootContext().stop();
                context.getRootContext().close();
            } catch (Exception e) {
                logger.error("Failed to stop root context.", e);
            }

            if (config.cleanSlate()) {
                try {
                    demolitioner.run();
                } catch (Exception e) {
                    logger.error("Demolition failed due to error.", e);
                }
            }
            logger.info("Voltron demolished in {} seconds", stopwatch.elapsed(TimeUnit.SECONDS));
        }
    }

    private void cleanArango(String name, ConfigurableWebApplicationContext context) {
        // Clean Arango
        try {
            GraphDatabaseDriverBuilder driverBuilder = context.getBean(GraphDatabaseDriverBuilder.class);
            GraphDBExecutor executor = context.getBean(GraphDBExecutor.class);
            GraphDatabaseDriver driver = driverBuilder.build(executor.getArangoDatabaseName(), "");
            driver.dropCollections();
        } catch (BeansException e) {
            // No config, expected for all except repository.
            logger.debug("No arango database config for {}. {}", name, e.getMessage());
        } catch (Exception e) {
            logger.error("Failed to remove arango database for {}", name, e);
        }
    }

    private void cleanSQL(Component component, ConfigurableWebApplicationContext context) {
        if (!component.getDbSchema().isPresent()) {
            // No schema - no cleaning.
            return;
        }

        // Clean SQL
        try {
            // If the context has a database configuration, delete the database.
            SQLDatabaseConfig config = context.getBean(SQLDatabaseConfig.class);
            DbCleanupRule.cleanDb(config.dsl(), component.getDbSchema().get());
        } catch (BeansException e) {
            // No SQLDatabaseConfig.
            logger.debug("No SQL database config for {}. {}", component.getShortName(), e.getMessage());
        } catch (Exception e) {
            logger.error("Failed to clean up database for {}", component.getShortName(), e);
        }

    }

    private void cleanConsul(String name, ConfigurableWebApplicationContext context) {
        try {
            Map<String, KeyValueStore> kvStores = context.getBeansOfType(KeyValueStore.class);
            kvStores.forEach((beanName, kvStore) -> {
                try {
                    if (!(kvStore instanceof VaultKeyValueStore)) {
                        kvStore.removeKeysWithPrefix("");
                    } else {
                        logger.debug("Skipping cleaning of vault kv store {}", beanName);
                    }
                } catch (Exception e) {
                    logger.error("Failed to remove consul data for kvstore {}", beanName, e);
                }
            });
        } catch (BeansException e) {
            // No config, or issue retrieving them
            logger.debug("No consul config for {}. {}", name, e.getMessage());
        } catch (Exception e) {
            logger.error("Failed to remove consul data for {}", name, e);
        }
    }

    /**
     * Return a channel to Voltron's gRPC server.
     * This will let you create gRPC stubs and call ANY gRPC service in ANY component in the
     * configuration.
     *
     * @return The channel.
     */
    @Nonnull
    public Channel getGrpcChannel() {
        return grpcChannel.ensureSet(() -> ComponentGrpcServer.newChannelBuilder("localhost",
                ComponentGrpcServer.get().getPort()).build());
    }

    private final SetOnce<Channel> grpcChannel = new SetOnce<>();

    /**
     * Get a message receiver, which can be used to listen to messages.
     *
     * @param topics The topics to listen on.
     * @param <T> The type of messages.
     * @return The {@link IMessageReceiver}.
     */
    @Nonnull
    public <T> IMessageReceiver<T> getMessageReceiver(@Nonnull final String... topics) {
        if (!config.isUseLocalBus()) {
            throw new IllegalArgumentException("Getting message receiver not supported when not"
                    + "using local bus.");
        }
        return LocalBus.getInstance().messageReceiver(Sets.newHashSet(topics), (foo) -> null);
    }

    /**
     * Get a message sender, which can be used to send messages.
     *
     * @param topic The topic to send messages to.
     * @param <T> The type of messages.
     * @return The {@link IMessageSender}.
     */
    public <T extends AbstractMessage> IMessageSender<T> getMessageSender(@Nonnull final String topic) {
        if (!config.isUseLocalBus()) {
            throw new IllegalArgumentException("Getting message sender not supported when not"
                    + "using local bus.");
        }
        return LocalBus.getInstance().messageSender(topic);
    }

    private final SetOnce<TurboApiClient> apiClient = new SetOnce<>();

    /**
     * Get an authenticated {@link TurboApiClient} that can be used to make external API calls.
     *
     * @return The {@link TurboApiClient}.
     */
    @Nonnull
    public TurboApiClient getApiClient() {
        if (context.getComponents().get(Component.API) == null) {
            throw new RuntimeException("API component not configured.");
        }

        return apiClient.ensureSet(() -> {
            TurboApiClient client = TurboApiClient.newBuilder()
                    .setHost("localhost")
                    .setPort(config.getServerHttpPort())
                    .setUser("administrator")
                    .setPassword("a")
                    .build();

            // Need to add a license.
            return client;
        });
    }

    private void loadComponentDiags(@Nonnull final Component component, @Nonnull final InputStream input) {
        AnnotationConfigWebApplicationContext context = this.context.getComponents().get(component);
        final String componentName = component.getShortName();
        if (context != null) {
            logger.info("Loading {} diags...", componentName);
            try {
                // This will throw an exception if the component doesn't support importing diags.
                final DiagnosticsControllerImportable diagCtrlr = context.getBean(DiagnosticsControllerImportable.class);
                // We use a close shield to prevent a close inside the diagnostics controller from closing the entire stream.
                // We don't create a NEW ZipInputStream to go over the contents of the nested zip,
                // because the controller does that internally.
                final InputStreamResource inputStreamResource = new InputStreamResource(new CloseShieldInputStream(input));
                final ResponseEntity<String> response = diagCtrlr.restoreInternalState(inputStreamResource);
                if (response.getStatusCode() == HttpStatus.OK) {
                    logger.info("Restored {} diags. Response:\n{}", componentName, response.getBody());
                } else {
                    logger.error("Failed to restore {} diags. Response: {} Message: {}",
                            componentName, response.getStatusCode(), response.getBody());
                }
            } catch (Exception e) {
                logger.error("Failed to load {} diags.", component.getShortName(), e);
            }
        } else {
            logger.warn("Not loading {} diags, because there is no {} component in the configuration Voltron.",
                    component.getShortName(), component.getShortName());
        }

    }

    /**
     * Trigger a topology broadcast. If the trigger attempt fails, retries for up to 60s.
     *
     * @throws InterruptedException If interrupted while retrying.
     * @throws RetriableOperationFailedException If there is an error with the broadcast.
     * @throws TimeoutException If not successful after 60s.
     */
    public void broadcastTopology() throws InterruptedException, RetriableOperationFailedException, TimeoutException {
        AnnotationConfigWebApplicationContext tpContext = context.getComponents().get(Component.TOPOLOGY_PROCESSOR);
        if (tpContext == null) {
            throw new RuntimeException("Failed to broadcast topology. No TP configured.");
        }
        TopologyServiceBlockingStub tpSvc = TopologyServiceGrpc.newBlockingStub(getGrpcChannel());
        RetriableOperation.newOperation(() ->
                tpSvc.requestTopologyBroadcast(TopologyBroadcastRequest.getDefaultInstance()))
                .retryOnException(e -> e instanceof StatusRuntimeException)
                .run(60, TimeUnit.SECONDS);
    }

    /**
     * Load diagnostics into components using their REST APIs.
     *
     * @param pathToDiagFile Path to the diagnostics file. This is the "top-level" diagnostics file.
     * @param components The components to load diags for.
     * @throws IOException If there is an error reading from the file.
     */
    public void loadDiags(@Nonnull final Path pathToDiagFile,
                          @Nonnull final Component... components) throws IOException {
        final Set<Component> targetComponents;
        if (components.length == 0) {
            // The defaults.
            targetComponents = Sets.newHashSet(Component.TOPOLOGY_PROCESSOR, Component.GROUP);
        } else {
            targetComponents = Sets.newHashSet(components);
        }

        File file = pathToDiagFile.toFile();
        if (file.exists() && file.isFile() && file.getName().endsWith(".zip")) {
            String fileName = file.getName();
            logger.info("Loading diags from file: {}", fileName);
            FileInputStream compressedInput = new FileInputStream(file);
            ZipInputStream input = new ZipInputStream(compressedInput);
            ZipEntry entry = null;
            while ( (entry = input.getNextEntry()) != null ) {
                if (entry.getName().startsWith("group") && targetComponents.contains(Component.GROUP)) {
                    loadComponentDiags(Component.GROUP, input);
                } else if (entry.getName().startsWith("topology-processor")) {
                    loadComponentDiags(Component.TOPOLOGY_PROCESSOR, input);
                }
            }
            logger.info("Finished loading diags from file: {}", fileName);
        } else {
            throw new RuntimeException("Invalid diags file: " + file.getAbsolutePath() + "."
                + " Must be the zipped diags from an appliance.");
        }
    }

    /**
     * Load a topology (from the topology processor's diagnostics .zip file).
     *
     * @param path The path to the topology processor diagnostic file.
     * @throws FileNotFoundException If the file is not found.
     */
    public void loadTopology(String path) throws FileNotFoundException {
        File file = Paths.get(path).toFile();
        if (file.exists() && file.isFile() && file.getName().endsWith(".zip")) {
            String fileName = file.getName();
            logger.info("Loading diags from file: {}", fileName);
            FileInputStream compressedInput = new FileInputStream(file);
            loadComponentDiags(Component.TOPOLOGY_PROCESSOR, compressedInput);
        } else {
            logger.error("Invalid diags file {}", path);

        }
    }
}
