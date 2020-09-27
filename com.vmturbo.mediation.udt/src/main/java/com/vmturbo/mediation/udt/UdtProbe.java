package com.vmturbo.mediation.udt;

import static com.vmturbo.group.api.GroupClientConfig.MAX_MSG_SIZE_BYTES;
import static com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity.CRITICAL;
import static java.util.Objects.isNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.ManagedChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;
import com.vmturbo.mediation.udt.config.ConnectionConfiguration;
import com.vmturbo.mediation.udt.config.ConnectionProperties;
import com.vmturbo.mediation.udt.config.PropertyReader;
import com.vmturbo.mediation.udt.config.UdtProbeConfiguration;
import com.vmturbo.mediation.udt.explore.Connection;
import com.vmturbo.mediation.udt.explore.DataProvider;
import com.vmturbo.mediation.udt.explore.DataRequests;
import com.vmturbo.mediation.udt.explore.RequestExecutor;
import com.vmturbo.mediation.udt.explore.UdtProbeExplorer;
import com.vmturbo.mediation.udt.explore.UdtSearchFilterResolver;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.ISupplyChainAwareProbe;
import com.vmturbo.platform.sdk.probe.ProbeConfiguration;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * A probe that creates topology based on topology data definitions pulled from the group component.
 * Business entities are created and attached to existing entities in the topology.
 */
public class UdtProbe implements IDiscoveryProbe<UdtProbeAccount>, ISupplyChainAwareProbe<UdtProbeAccount> {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * This is the maximum number of threads which the Probe can create for executing tasks
     * in the discovery cycle of a controller.
     */
    private static final int MAX_THREADS_ALLOWED = 10;

    /**
     * We need a minimum number of threads (assuming the user does not set a specific
     * pool size). This is needed as Kubernetes lies about the number available cores
     * and causes use to size thread pools too small.
     */
    private static final int MIN_REQUIRED_THREADS_NEEDED = 4;

    /**
     * Used as a 'VENDOR' value in EntityDTO.
     */
    public static final String UDT_PROBE_TAG = "UDT";
    private Connection connection;
    private IPropertyProvider propertyProvider;

    /**
     * Creates a {@link Connection} instance, which contains gRpc channels to the
     * Group and Repository components.
     *
     * @param config - connection configuration.
     * @return an instance of {@link Connection}.
     */
    @Nonnull
    @VisibleForTesting
    Connection createProbeConnection(@Nonnull ConnectionConfiguration config) {
        final ConnectionProperties properties = config.getProperties();
        final ManagedChannel groupChannel = ComponentGrpcServer.newChannelBuilder(
                properties.getGroupHost(), properties.getgRpcPort(), MAX_MSG_SIZE_BYTES)
                .build();
        final ManagedChannel repositoryChannel = ComponentGrpcServer.newChannelBuilder(
                properties.getRepositoryHost(), properties.getgRpcPort())
                .keepAliveTime(properties.getgRpcPingIntervalSeconds(), TimeUnit.SECONDS)
                .build();
        final ManagedChannel topologyProcessorChannel = ComponentGrpcServer.newChannelBuilder(
                properties.getTopologyProcessorHost(), properties.getgRpcPort())
                .keepAliveTime(properties.getgRpcPingIntervalSeconds(), TimeUnit.SECONDS)
                .build();
        final TopologyProcessor topologyProcessorApi = config.getTpConfig().topologyProcessorRpcOnly();
        LOGGER.info("UDT Probe connection created.");
        return new Connection(groupChannel, repositoryChannel, topologyProcessorChannel, topologyProcessorApi);
    }

    @VisibleForTesting
    void initializeConnection(@Nonnull Connection connection) {
        this.connection = connection;
    }

    @Override
    public void initialize(@Nonnull IProbeContext probeContext, @Nullable ProbeConfiguration probeConfiguration) {
        propertyProvider = probeContext.getPropertyProvider();
        final AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
        final PropertyReader propertyReader = new PropertyReader(context);
        try {
            final ConnectionConfiguration configuration = propertyReader.getConnectionConfiguration();
            final Connection connection = createProbeConnection(configuration);
            initializeConnection(connection);
        } catch (ContextConfigurationException e) {
            LOGGER.error("Initialization exception.", e);
        }
    }

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        return new UdtSupplyChain().getSupplyChainDefinition();
    }

    @Nonnull
    @Override
    public Class<UdtProbeAccount> getAccountDefinitionClass() {
        return UdtProbeAccount.class;
    }

    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull UdtProbeAccount account) {
        final ValidationResponse.Builder response = ValidationResponse.newBuilder();
        if (isNull(connection)) {
            response.addErrorDTO(Discovery.ErrorDTO.newBuilder()
                    .setSeverity(CRITICAL)
                    .setDescription("Probe Connection is NULL.")
                    .build());
        }
        return response.build();
    }

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull UdtProbeAccount account) {
        LOGGER.info("{} discovery started.", account.getTargetName());
        ExecutorService executor = null;
        try {
            if (isNull(connection)) {
                throw new Exception("Probe Connection is NULL.");
            }
            final long startTime = System.currentTimeMillis();
            final UdtProbeConfiguration probeConfiguration = new UdtProbeConfiguration(propertyProvider);
            executor = buildPool(probeConfiguration.getPoolSize());
            final Set<UdtEntity> entities = new UdtProbeExplorer(buildDataProvider(connection), executor).exploreDataDefinition();
            final DiscoveryResponse response = new UdtProbeConverter(entities).createDiscoveryResponse();
            LOGGER.info("Discovery info:\n{}", getDiscoveryInfo(entities, startTime));
            LOGGER.info("{} discovery finished.", account.getTargetName());
            if (LOGGER.isTraceEnabled()) {
                entities.forEach(entity -> {
                    LOGGER.trace("Discovered UDT entity: {}", entity);
                });
                LOGGER.trace("Discovery Response: {}", response);
            }
            return response;
        } catch (Exception e) {
            LOGGER.error("{} discovery error.", account.getTargetName(), e);
            return SDKUtil.createDiscoveryError(e.getMessage());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    @Nonnull
    private String getDiscoveryInfo(@Nonnull Set<UdtEntity> entities, long startTime) {
        final long durationSec = (System.currentTimeMillis() - startTime) / 1000;
        String info = String.format("Discovered %d entities in %s seconds.\n",
                entities.size(), durationSec);
        final Map<EntityType, Set<UdtEntity>> typeMap = entities.stream().reduce(new HashMap<>(),
                (map, entity) -> {
                    if (!map.containsKey(entity.getEntityType())) {
                        map.put(entity.getEntityType(), Sets.newHashSet());
                    }
                    map.get(entity.getEntityType()).add(entity);
                    return map;
                }, (m1, m2) -> null);
        for (Entry<EntityType, Set<UdtEntity>> entry : typeMap.entrySet()) {
            info += String.format("%s: %d\n", entry.getKey().name(), entry.getValue().size());
        }
        return info;
    }

    @Nonnull
    @VisibleForTesting
    DataProvider buildDataProvider(@Nonnull Connection connection) {
        final RequestExecutor requestExecutor = new RequestExecutor(connection);
        final DataRequests dataRequests = new DataRequests();
        final SearchFilterResolver searchFilterResolver
                = new UdtSearchFilterResolver(connection, requestExecutor, dataRequests);
        return new DataProvider(requestExecutor, dataRequests, searchFilterResolver);
    }

    /**
     * Destroys the Probe, close connections.
     */
    @Override
    public void destroy() {
        if (connection != null) {
            connection.release();
        }
    }

    private static ExecutorService buildPool(int poolSize) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("UDT-%d").build();
        int coreThreadPoolSize = (poolSize == -1) ? Runtime.getRuntime().availableProcessors() : poolSize;
        coreThreadPoolSize = Math.min(coreThreadPoolSize, MAX_THREADS_ALLOWED);
        if (poolSize == -1) {
            // for defaults, make sure we get enough threads.
            // Kubernetes lies and tells the JVM we only have a single core.
            coreThreadPoolSize = Math.max(coreThreadPoolSize, MIN_REQUIRED_THREADS_NEEDED);
        }
        final ThreadPoolExecutor pool = new ThreadPoolExecutor(coreThreadPoolSize,
                // no more than this many threads
                coreThreadPoolSize,
                // keep threads around for 15 seconds after they are done
                15L, TimeUnit.SECONDS,
                // queue tasks (unbounded) if no threads available.
                new LinkedBlockingQueue<>(), threadFactory
        );
        // This will allow the MAX_THREAD_POOL_SIZE core pool threads to time out
        // and be destroyed when they are have not been used for 30 seconds.
        pool.allowCoreThreadTimeOut(true);
        LOGGER.info("Created discovery thread pool of size {}", coreThreadPoolSize);
        return pool;
    }
}
