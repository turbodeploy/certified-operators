package com.vmturbo.topology.processor.analysis;

import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.topology.AnalysisDTO;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.TopologyHandler.TopologyBroadcastInfo;

/**
 * See: topology/AnalysisDTO.proto
 */
public class AnalysisService extends AnalysisServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyHandler topologyHandler;

    private final EntityStore entityStore;

    private final IdentityProvider identityProvider;

    private final RepositoryClient repository;

    private final Clock clock;

    public AnalysisService(@Nonnull final TopologyHandler topologyHandler,
                           @Nonnull final EntityStore entityStore,
                           @Nonnull final IdentityProvider identityProvider,
                           @Nonnull final RepositoryClient repositoryClient,
                           @Nonnull final Clock clock) {
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.repository = Objects.requireNonNull(repositoryClient);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public void startAnalysis(AnalysisDTO.StartAnalysisRequest request,
                              StreamObserver<AnalysisDTO.StartAnalysisResponse> responseObserver) {
        Collection<TopologyEntityDTO> topology;
        long topologyId;
        if (!request.hasTopologyId()) {
            logger.info("Received analysis request for the real-time topology.");
            topology = entityStore.constructTopology().values().stream()
                .map(TopologyEntityDTO.Builder::build)
                .collect(Collectors.toList());
            // We need to assign a new topology ID to this latest topology.
            topologyId = identityProvider.generateTopologyId();
        } else {
            topologyId = request.getTopologyId();
            logger.info("Received analysis request for projected topology {}", topologyId);
            // we need to gather the entire topology in order to perform editing below
            Iterable<RepositoryDTO.RetrieveTopologyResponse> dtos =
                    () -> repository.retrieveTopology(topologyId);
            topology = StreamSupport.stream(dtos.spliterator(), false)
                    .map(RepositoryDTO.RetrieveTopologyResponse::getEntitiesList)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            logger.info("Received analysis request for a topology {}. {} entities received.",
                    topologyId, topology.size());
        }

        // Apply changes if necessary.
        // TODO (roman, Dec 7 2016): We could do this more efficiently by making
        // the changes at broadcast-time. However, since that implementation is likely
        // to be more complex than this one it's better to postpone the optimization
        // until the edit functionality is more fleshed out (e.g. with group recalculation,
        // etc.) before doing it.
        if (request.getScenarioChangeCount() > 0) {
            topology = editTopology(topology, request.getScenarioChangeList());
        }

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(request.getPlanId())
                .setTopologyId(topologyId)
                .setCreationTime(clock.millis())
                .setTopologyType(TopologyType.PLAN)
                .build();

        try {
            final TopologyBroadcastInfo broadcastInfo =
                    topologyHandler.broadcastTopology(topologyInfo,
                            topology.stream());

            responseObserver.onNext(StartAnalysisResponse.newBuilder()
                    .setEntitiesBroadcast(broadcastInfo.getEntityCount())
                    .setTopologyId(broadcastInfo.getTopologyId())
                    .setTopologyContextId(broadcastInfo.getTopologyContextId())
                    .build());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            responseObserver.onError(Status.INTERNAL.asException());
            Thread.interrupted();
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private Collection<TopologyEntityDTO> editTopology(@Nonnull final Collection<TopologyEntityDTO> topology,
                                                @Nonnull final List<ScenarioChange> changes) {
        final Map<Long, TopologyAddition> additions = new HashMap<>();
        final Map<Long, TopologyRemoval> removals = new HashMap<>();

        changes.forEach(change -> {
            if (change.hasTopologyAddition()) {
                final TopologyAddition addition = change.getTopologyAddition();
                if (addition.hasEntityId()) {
                    additions.put(addition.getEntityId(), addition);
                } else {
                    logger.warn("Unimplemented handling for topology addition with {}",
                            addition.getEntityOrTemplateOrGroupIdCase());
                }
            } else if (change.hasTopologyRemoval()) {
                final TopologyRemoval removal = change.getTopologyRemoval();
                if (removal.hasEntityId()) {
                    removals.put(removal.getEntityId(), removal);
                } else {
                    logger.warn("Unimplemented handling for topology removal with {}",
                            removal.getEntityOrGroupIdCase());
                }
            } else if (change.hasTopologyReplace()) {
                logger.warn("Handling for topology replacements is unimplemented.");
            } else {
                logger.warn("Unimplemented handling for change of type {}", change.getDetailsCase());
            }
        });

        final Set<TopologyEntityDTO> updatedEntities = new HashSet<>();

        topology.forEach(entity -> {
            TopologyAddition addition = additions.get(entity.getOid());
            if (addition != null) {
                final int addCount = addition.hasAdditionCount() ? addition.getAdditionCount() : 1;
                for (int i = 0; i < addCount; ++i) {
                    // TODO: We need to remove provider Id of commodity bought for all clone entities,
                    // and it requires to refactor TopologyConverter class in order to convert related
                    // commodity bought to correct Biclique commodity bought. (see OM-25520)
                    final TopologyEntityDTO clone = TopologyEntityDTO.newBuilder(entity)
                            // This may get confusing if there are multiple clone entities,
                            // but the display name isn't supposed to uniquely identify
                            // the entity anyway. The important thing is to indicate
                            // that this is a clone.
                            .setDisplayName(entity.getDisplayName() + " - Clone")
                            .setOid(identityProvider.getCloneId(entity))
                            .build();
                    updatedEntities.add(clone);
                }
            }

            // Preserve the entity in the updated set unless it was
            // specifically targeted for removal.
            if (!removals.containsKey(entity.getOid())) {
                updatedEntities.add(entity);
            }
        });

        return updatedEntities;
    }
}
