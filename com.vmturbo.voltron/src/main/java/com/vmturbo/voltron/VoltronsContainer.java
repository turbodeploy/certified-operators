package com.vmturbo.voltron;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.springframework.beans.BeansException;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceBlockingStub;
import com.vmturbo.communication.CommunicationException;
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
import com.vmturbo.securekvstore.VaultKeyValueStore;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
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

    private void cleanSQL(Component component, ConfigurableWebApplicationContext context) {
        if (!component.getDbSchema().isPresent()) {
            // No schema - no cleaning.
            return;
        }

        // Clean SQL
        try {
            // If the context has a database configuration, delete the database.
            SQLDatabaseConfig config = context.getBean(SQLDatabaseConfig.class);
            Flyway flyway = new Flyway();
            flyway.setSchemas(component.getDbSchema().get().getName());
            flyway.setDataSource(config.dataSource());
            flyway.clean();
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
            try (ZipInputStream input = new ZipInputStream(compressedInput)) {
                ZipEntry entry = null;
                while ((entry = input.getNextEntry()) != null) {
                    if (entry.getName().startsWith("group") && targetComponents.contains(Component.GROUP)) {
                        loadComponentDiags(Component.GROUP, input);
                    } else if (entry.getName().startsWith("topology-processor")) {
                        loadComponentDiags(Component.TOPOLOGY_PROCESSOR, input);
                    }
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

    /**
     * Use this to broadcast a topology saved by {@link VoltronsContainer#saveTopologyToFile(String)}
     * back on the bus.
     *
     * @param path The path to the saved topology binary file.
     *
     * @throws IOException If there is an error reading.
     * @throws CommunicationException If there is an error sending.
     * @throws InterruptedException If there is an interrupt while waiting.
     */
    public void broadcastTopologyFromFile(@Nonnull final String path)
            throws IOException, CommunicationException, InterruptedException {
        File file = new File(path);
        int lineCnt = 0;
        IMessageSender<Topology> receiver = getMessageSender(TopologyProcessorClient.TOPOLOGY_LIVE);
        try (InputStream is = FileUtils.openInputStream(file)) {
            try {
                while (true) {
                    Topology e = Topology.parseDelimitedFrom(is);
                    if (e == null) {
                        break;
                    }
                    lineCnt++;
                    receiver.sendMessage(e);
                    if (lineCnt % 10000 == 0) {
                        logger.info("Processed {}", lineCnt);
                    }
                }
            } catch (IOException e) {
                // Noop.
                logger.error(e);
            }
        }
    }

    /**
     * Trigger a broadcast, and save the next broadcast topology into a file.
     * The file will be saved in binary format, with a series of delimited {@link Topology} objects.
     *
     * @param path The path to save to. Must be a file (not a directory). If a file exists on this
     * path, the file will be overwritten.
     *
     * @throws InterruptedException If interrupted waiting for broadcast.
     * @throws TimeoutException If timed out waiting for successful broadcast.
     * @throws RetriableOperationFailedException If we fail to broadcast the topology (e.g.
     *                                           server is unavailable).
     * @throws IOException If there is an error writing the topology to file.
     */
    public void saveTopologyToFile(String path)
            throws InterruptedException, TimeoutException,
            RetriableOperationFailedException, IOException {
        File file = Paths.get(path).toFile();
        if (file.isDirectory()) {
            throw new IllegalArgumentException("Must be a file.");
        }
        file.getParentFile().mkdirs();
        file.delete();
        final TopologySaveState saveState;
        try {
            saveState = new TopologySaveState(new FileOutputStream(path));
        } catch (FileNotFoundException e) {
            // Shouldn't happen because we made sure it's not a directory above.
            throw new IllegalStateException(e);
        }
        IMessageReceiver<Topology> receiver = getMessageReceiver(TopologyProcessorClient.TOPOLOGY_LIVE);
        receiver.addListener(((topology, runnable, spanContext) -> {
            if (topology.hasStart() && saveState.topologyId == 0) {
                saveState.topologyId = topology.getTopologyId();
            }
            if (saveState.topologyId == topology.getTopologyId()) {
                try {
                    topology.writeDelimitedTo(saveState.fos);
                    if (topology.hasEnd()) {
                        saveState.fos.flush();
                        saveState.fos.close();
                        saveState.future.complete(null);
                    }
                } catch (IOException e) {
                    saveState.future.completeExceptionally(e);
                }
            }
        }));
        broadcastTopology();
        try {
            saveState.future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            } else {
                // Shouldn't happen.
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Utility class to capture information about the topology we are saving to a file
     * via {@link VoltronsContainer#saveTopologyToFile(String)}.
     */
    private static class TopologySaveState {
        private final FileOutputStream fos;

        private long topologyId = 0;

        private final CompletableFuture<Void> future = new CompletableFuture<>();

        private TopologySaveState(FileOutputStream fos) {
            this.fos = fos;
        }
    }
}
