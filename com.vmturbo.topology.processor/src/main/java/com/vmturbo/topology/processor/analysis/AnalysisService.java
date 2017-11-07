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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.topology.AnalysisDTO;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.TopologyHandler.TopologyBroadcastInfo;

/**
 * See: topology/AnalysisDTO.proto.
 */
public class AnalysisService extends AnalysisServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyHandler topologyHandler;

    private final EntityStore entityStore;

    private final IdentityProvider identityProvider;

    private final RepositoryClient repository;

    private final Clock clock;

    private final TemplateConverterFactory templateConverterFactory;

    public AnalysisService(@Nonnull final TopologyHandler topologyHandler,
                           @Nonnull final EntityStore entityStore,
                           @Nonnull final IdentityProvider identityProvider,
                           @Nonnull final RepositoryClient repositoryClient,
                           @Nonnull final Clock clock,
                           @Nonnull final TemplateConverterFactory templateConverterFactory) {

        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.repository = Objects.requireNonNull(repositoryClient);
        this.clock = Objects.requireNonNull(clock);
        this.templateConverterFactory = Objects.requireNonNull(templateConverterFactory);
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
            topology = editTopology(topology, request.getScenarioChangeList(), identityProvider,
                templateConverterFactory);
        }

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(request.getPlanId())
                .setTopologyId(topologyId)
                .setCreationTime(clock.millis())
                .setTopologyType(TopologyType.PLAN)
                .build();

        try {
            // TODO destinguish between user plan and scheduled plan
            final TopologyBroadcastInfo broadcastInfo =
                    topologyHandler.broadcastUserPlanTopology(topologyInfo,
                            topology);

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
        } catch (CommunicationException e) {
            responseObserver.onError(Status.INTERNAL.asException());
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    @Nonnull
    protected static Collection<TopologyEntityDTO> editTopology(
                    @Nonnull final Collection<TopologyEntityDTO> topology,
                    @Nonnull final List<ScenarioChange> changes,
                    @Nonnull final IdentityProvider identityProvider,
                    @Nonnull final TemplateConverterFactory templateConverterFactor) {
        final Map<Long, Long> entityAdditions = new HashMap<>();
        final Set<Long> entityToRemove = new HashSet<>();
        final Map<Long, Long> templateToAdd = new HashMap<>();

        changes.forEach(change -> {
            if (change.hasTopologyAddition()) {
                final TopologyAddition addition = change.getTopologyAddition();
                if (addition.hasEntityId()) {
                    addTopologyAdditionCount(entityAdditions, addition, addition.getEntityId());
                } else if (addition.hasTemplateId()) {
                    addTopologyAdditionCount(templateToAdd, addition, addition.getTemplateId());
                } else {
                    logger.warn("Unimplemented handling for topology addition with {}",
                            addition.getEntityOrTemplateOrGroupIdCase());
                }
            } else if (change.hasTopologyRemoval()) {
                final TopologyRemoval removal = change.getTopologyRemoval();
                if (removal.hasEntityId()) {
                    entityToRemove.add(removal.getEntityId());
                } else {
                    logger.warn("Unimplemented handling for topology removal with {}",
                            removal.getEntityOrGroupIdCase());
                }
            } else if (change.hasTopologyReplace()) {
                final TopologyReplace replace = change.getTopologyReplace();
                templateToAdd.put(replace.getAddTemplateId(),
                    templateToAdd.getOrDefault(replace.getAddTemplateId(), 0L) + 1);
                if (replace.hasRemoveEntityId()) {
                    entityToRemove.add(replace.getRemoveEntityId());
                } else {
                    logger.warn("Unimplemented handling for topology removal with {}",
                        replace.getEntityOrGroupRemovalIdCase());
                }

            } else {
                logger.warn("Unimplemented handling for change of type {}", change.getDetailsCase());
            }
        });

        final Set<TopologyEntityDTO> updatedEntities = new HashSet<>();

        topology.forEach(entity -> {
            final long addCount = entityAdditions.getOrDefault(entity.getOid(), 0L);
            for (int i = 0; i < addCount; ++i) {
                updatedEntities.add(clone(entity, identityProvider, i));
            }

            // Preserve the entity in the updated set unless it was
            // specifically targeted for removal or need to be replaced.
            if (!entityToRemove.contains(entity.getOid())) {
                updatedEntities.add(entity);
            }
        });
        addTemplateTopologyEntities(templateToAdd, updatedEntities, templateConverterFactor);
        return updatedEntities;
    }

    /**
     * Create a clone of a topology entity, modifying some values, including
     * oid, display name, and unplacing the shopping lists.
     *
     * @param entity source topology entity
     * @param identityProvider used to generate an oid for the clone
     * @param cloneCounter used in the display name
     * @return the cloned entity
     */
    private static TopologyEntityDTO clone(TopologyEntityDTO entity,
            @Nonnull final IdentityProvider identityProvider,
            int cloneCounter) {
        final TopologyEntityDTO.Builder cloneBuilder =
            TopologyEntityDTO.newBuilder(entity)
                .clearCommoditiesBoughtFromProviders();
        // unplace all commodities bought, so that the market creates a Placement action for them.
        Map<Long, Long> oldProvidersMap = Maps.newHashMap();
        long noProvider = 0;
        for (CommoditiesBoughtFromProvider bought :
            entity.getCommoditiesBoughtFromProvidersList()) {
            long oldProvider = bought.getProviderId();
            cloneBuilder.addCommoditiesBoughtFromProviders(
                bought.toBuilder().setProviderId(--noProvider).build());
            oldProvidersMap.put(noProvider, oldProvider);
        }
        Map<String, String> entityProperties =
            Maps.newHashMap(cloneBuilder.getEntityPropertyMapMap());
        if (!oldProvidersMap.isEmpty()) {
            // TODO: OM-26631 - get rid of unstructured data and Gson
            entityProperties.put("oldProviders", new Gson().toJson(oldProvidersMap));
        }
        final TopologyEntityDTO clone = cloneBuilder
            .setDisplayName(entity.getDisplayName() + " - Clone #" + cloneCounter)
            .setOid(identityProvider.getCloneId(entity))
            .putAllEntityPropertyMap(entityProperties)
            .build();
        return clone;
    }

    /**
     * Add all addition topology entities which converted from templates
     *
     * @param templateAdditions contains all addition templates and the count need to add.
     * @param updatedEntities contains updated topology entities which will be broadcast.
     */
    private static void addTemplateTopologyEntities(@Nonnull Map<Long, Long> templateAdditions,
                                                    @Nonnull Set<TopologyEntityDTO> updatedEntities,
                                                    @Nonnull final TemplateConverterFactory templateConverterFactor) {
        // Check if there are templates additions
        if (templateAdditions.isEmpty()) {
            return;
        }
        final Set<TopologyEntityDTO> topologyEntityDTOS =
            templateConverterFactor
                .generateTopologyEntityFromTemplates(templateAdditions.keySet(), templateAdditions);
        updatedEntities.addAll(topologyEntityDTOS);
    }

    private static void addTopologyAdditionCount(@Nonnull final Map<Long, Long> additionMap,
                                          @Nonnull TopologyAddition addition,
                                          long key) {
        final long additionCount =
            addition.hasAdditionCount() ? addition.getAdditionCount() : 1L;
        additionMap.put(key, additionMap.getOrDefault(key, 0L) + additionCount);
    }
}
