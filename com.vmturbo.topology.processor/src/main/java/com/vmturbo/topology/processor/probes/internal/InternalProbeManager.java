package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import static com.vmturbo.topology.processor.probes.internal.InternalProbeValidator.isProbeValid;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Manager class for working with an internal probe.
 */
public class InternalProbeManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final RemoteMediationServer server;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final GroupServiceBlockingStub groupService;
    private final EntityRetriever entityRetriever;

    /**
     * Constructor.
     *
     * @param server          - Remote mediation (SDK) server.
     * @param probeStore      - class for registering probes.
     * @param targetStore     - class for CRUD operations on registered targets.
     * @param groupService    - API interface for Group component.
     * @param entityRetriever - retriever for entities from repository component.
     */
    @ParametersAreNonnullByDefault
    public InternalProbeManager(RemoteMediationServer server, ProbeStore probeStore,
                                TargetStore targetStore, GroupServiceBlockingStub groupService,
                                EntityRetriever entityRetriever) {
        this.server = server;
        this.probeStore = probeStore;
        this.targetStore = targetStore;
        this.groupService = groupService;
        this.entityRetriever = entityRetriever;
    }

    /**
     * The method creates internal probes for TP.
     */
    public void createProbes() {
        createInternalProbeUDE();
    }

    /**
     * Creates 'UserDefinedEntities' probe.
     */
    private void createInternalProbeUDE() {
        final UserDefinedEntitiesProbeRetrieval udeProbeRetrieval
                = new UserDefinedEntitiesProbeRetrieval(groupService, entityRetriever);
        final InternalProbeDefinition probeDefinition
                = new UserDefinedEntitiesProbeDefinition(udeProbeRetrieval);
        createInternalProbe(probeDefinition);
    }

    private void createInternalProbe(@Nonnull InternalProbeDefinition definition) {
        if (!isProbeValid(definition.getProbeInfo())) {
            LOGGER.warn("Internal probe validation failed.");
            return;
        }
        server.registerTransport(createContainerInfo(definition.getProbeInfo()), definition.getTransport());
        final Optional<Long> optional = probeStore.getProbeIdForType(definition.getProbeType());
        // If probe successfully created.
        if (optional.isPresent()) {
            long probeId = optional.get();
            LOGGER.info("Internal probe created: {}", probeId);
            createInternalProbeTarget(probeId, definition);
        } else {
            LOGGER.warn("Internal probe creating failed.");
        }
    }

    @Nonnull
    private ContainerInfo createContainerInfo(@Nonnull ProbeInfo info) {
        return ContainerInfo.newBuilder().addProbes(info).build();
    }

    private void createInternalProbeTarget(long probeId, @Nonnull InternalProbeDefinition definition) {
        try {
            // If a target is not created yet.
            if (targetStore.getProbeTargets(probeId).isEmpty()) {
                final TargetSpec targetSpec = definition.getProbeTarget(probeId);
                targetStore.createTarget(targetSpec);
                LOGGER.info("Target for internal probe {} created.", probeId);
            }
        } catch (InvalidTargetException | IdentityStoreException | DuplicateTargetException e) {
            LOGGER.error("Error while creating target for probe {}: {}", probeId, e.getMessage());
        }
    }


}
