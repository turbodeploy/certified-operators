package com.vmturbo.mediation.udt;

import static com.vmturbo.group.api.GroupClientConfig.MAX_MSG_SIZE_BYTES;
import static com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity.CRITICAL;
import static java.util.Objects.isNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import io.grpc.ManagedChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;
import com.vmturbo.mediation.udt.config.ConnectionProperties;
import com.vmturbo.mediation.udt.config.PropertyReader;
import com.vmturbo.mediation.udt.explore.Connection;
import com.vmturbo.mediation.udt.explore.DataProvider;
import com.vmturbo.mediation.udt.explore.DataRequests;
import com.vmturbo.mediation.udt.explore.RequestExecutor;
import com.vmturbo.mediation.udt.explore.UdtProbeExplorer;
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

/**
 * A probe that creates topology based on topology data definitions pulled from the group component.
 * Business entities are created and attached to existing entities in the topology.
 */
public class UdtProbe implements IDiscoveryProbe<UdtProbeAccount>, ISupplyChainAwareProbe<UdtProbeAccount> {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Used as a 'VENDOR' value in EntityDTO.
     */
    public static final String UDT_PROBE_TAG = "UDT";
    private Connection connection;

    /**
     * Creates a {@link Connection} instance, which contains gRpc channels to the
     * Group and Repository components.
     *
     * @param properties - connection properties.
     * @return an instance of {@link Connection}.
     */
    @Nonnull
    @VisibleForTesting
    Connection createProbeConnection(@Nonnull ConnectionProperties properties) {
        final ManagedChannel groupChannel = ComponentGrpcServer.newChannelBuilder(
                properties.getGroupHost(), properties.getgRpcPort(), MAX_MSG_SIZE_BYTES)
                .build();
        final ManagedChannel repositoryChannel = ComponentGrpcServer.newChannelBuilder(
                properties.getRepositoryHost(), properties.getgRpcPort())
                .keepAliveTime(properties.getgRpcPingIntervalSeconds(), TimeUnit.SECONDS)
                .build();
        LOGGER.info("UDT Probe connection created.");
        return new Connection(groupChannel, repositoryChannel);
    }

    @VisibleForTesting
    void initializeConnection(@Nonnull Connection connection) {
        this.connection = connection;
    }

    @Override
    public void initialize(@Nonnull IProbeContext probeContext, @Nullable ProbeConfiguration probeConfiguration) {
        final AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
        final PropertyReader propertyReader = new PropertyReader(context);
        try {
            final ConnectionProperties properties = propertyReader.getConnectionProperties();
            final Connection connection = createProbeConnection(properties);
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
        try {
            if (isNull(connection)) {
                throw new Exception("Probe Connection is NULL.");
            }
            final Set<UdtEntity> entities = new UdtProbeExplorer(buildDataProvider(connection)).exploreDataDefinition();
            final DiscoveryResponse response = new UdtProbeConverter(entities).createDiscoveryResponse();
            LOGGER.info("Discovery info:\n{}", getDiscoveryInfo(entities));
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
        }
    }

    @Nonnull
    private String getDiscoveryInfo(@Nonnull Set<UdtEntity> entities) {
        String info = String.format("Discovered %d entities.\n", entities.size());
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
        return new DataProvider(requestExecutor, dataRequests);
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
}
