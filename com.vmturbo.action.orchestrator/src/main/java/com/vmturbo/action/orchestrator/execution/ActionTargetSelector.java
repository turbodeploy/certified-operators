package com.vmturbo.action.orchestrator.execution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStoreFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;

/**
 * Select the target/probe to execute an action against.
 * It is also responsible for breaking ties when multiple targets are eligible for action execution.
 */
public class ActionTargetSelector {

    private static final ActionTargetInfo SHOW_ONLY = ImmutableActionTargetInfo.builder()
        .supportingLevel(SupportLevel.SHOW_ONLY)
        .build();

    private static final Logger logger = LogManager.getLogger();

    private final TargetInfoResolver targetInfoResolver;

    /**
     * A client for making remote calls to the repository service to retrieve entity data.
     */
    private final RepositoryServiceBlockingStub repositoryService;

    /**
     * The context ID for the realtime market.
     * Used when making remote calls to the repository service.
     */
    private final long realtimeTopologyContextId;

    /**
     * Constructor.
     *
     * @param probeCapabilityCache To get the target-specific action capabilities
     * @param actionConstraintStoreFactory the factory which has access to all action constraint stores
     * @param repositoryProcessorChannel channel to access repository
     * @param realtimeTopologyContextId the context ID of the realtime market
     */
    public ActionTargetSelector(@Nonnull final ProbeCapabilityCache probeCapabilityCache,
            @Nonnull final ActionConstraintStoreFactory actionConstraintStoreFactory,
            @Nonnull final Channel repositoryProcessorChannel,
            final long realtimeTopologyContextId) {
        this.targetInfoResolver =
                new TargetInfoResolver(probeCapabilityCache, actionConstraintStoreFactory,
                        new EntityAndActionTypeBasedEntitySelector());
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repositoryProcessorChannel);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Describes the target resolution result for a particular {@link ActionDTO.Action}.
     */
    @Value.Immutable
    public interface ActionTargetInfo {
        /**
         * The support level for the action in the Turbonomic system.
         */
        SupportLevel supportingLevel();

        /**
         * The OID of the target that should execute this action. Should always be set if
         * {@link ActionTargetInfo#supportingLevel()} is {@link SupportLevel#SUPPORTED}. May not be
         * set otherwise.
         */
        Optional<Long> targetId();

        /**
         * The pre-requisites for an action.
         *
         * @return a set of {@link Prerequisite}s
         */
        Set<Prerequisite> prerequisites();

        /**
         * Checks if action is disruptive.
         *
         * @return True for disruptive action. If probe hasn't provided data about action
         * disruptiveness then it returns {@code null}.
         */
        @Nullable
        Boolean disruptive();

        /**
         * Checks if action is reversible.
         *
         * @return True for reversible action. If probe hasn't provided data about action
         * reversibility then it returns {@code null}.
         */
        @Nullable
        Boolean reversible();
    }

    /**
     * Return the {@link ActionTargetInfo} for an action. Makes an RPC call internally.
     * If you need to do this for more than
     * one action, use {@link ActionTargetSelector#getTargetsForActions(Stream, EntitiesAndSettingsSnapshot)}.
     *
     * @param action The {@link ActionDTO.Action} generated by the market.
     * @param entitySettingsCache an entity snapshot factory used for creating entity snapshot.
     *                            It is now only used for creating empty entity snapshot.
     * @return An {@link ActionTargetInfo} describing the support + target id for the action.
     */
    @Nonnull
    public ActionTargetInfo getTargetForAction(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache) {
        return getTargetsForActions(Stream.of(action), entitySettingsCache.emptySnapshot()).get(
                action.getId());
    }

    /**
     * Returns an {@link ActionTargetInfo} for each provided action.
     * This is the "bulk" version of
     * {@link ActionTargetSelector#getTargetForAction(ActionDTO.Action, EntitiesAndSettingsSnapshotFactory)}.
     *
     * <p>This method retrieves the target resolution data (support level and targetId) for each
     * action in the actions Stream. Entity data for entities associated with each action is required,
     * and will be retrieved from the entityMap (if provided) or else will be retrieved using a remote
     * call to Repository which will retrieve entity data from the realtime market.</p>
     *
     * <p>TODO: provide a flag to cause an explanation/failure to be reported for unsupported actions
     * where applicable.</p>
     *
     * @param actions Stream of action recommendations.
     * @param snapshot The snapshot of entities. If the snapshot is empty, the data will instead
     *                 be retrieved by remote calls
     * @return Map of (action id) to ({@link ActionTargetInfo}) for the action, for each
     *         action in the input.
     */
    @Nonnull
    public Map<Long, ActionTargetInfo> getTargetsForActions(
            @Nonnull final Stream<ActionDTO.Action> actions,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        final List<ActionDTO.Action> actionList = actions.collect(Collectors.toList());
        final Map<Long, ActionPartialEntity> actionPartialEntityMap;
        if (snapshot.getEntityMap().isEmpty()) {
            // We need to fetch the partial action entities from repository.
            actionPartialEntityMap =
                    getRealtimeActionEntities(ActionDTOUtil.getInvolvedEntityIds(actionList));
        } else {
            actionPartialEntityMap = snapshot.getEntityMap();
        }

        final Map<Long, ActionTargetInfo> targetsForActions = new HashMap<>();
        try {
            actionList.forEach(action -> targetsForActions.put(action.getId(),
                    targetInfoResolver.getTargetInfoForAction(action, actionPartialEntityMap,
                            snapshot)));
        } catch (StatusRuntimeException e) {
            // If we can't get the information from the topology processor, default the support
            // level for all actions that have targets should be SHOW_ONLY.
            logger.error("Failed to get entity infos from the topology processor. Error: {}",
                    e.getMessage());
            actionList.forEach(
                    action -> targetsForActions.computeIfAbsent(action.getId(), key -> SHOW_ONLY));
        }
        return targetsForActions;
    }

    /**
     * Return the {@link ActionPartialEntity} for the given entity ids from the realtime topology.
     *
     * @param entityIds The id of the entities.
     * @return a {@link Map} with an {@link ActionPartialEntity} per entity id.
     */
    private Map<Long, ActionPartialEntity> getRealtimeActionEntities(Set<Long> entityIds) {
        RetrieveTopologyEntitiesRequest getEntitiesrequest = RetrieveTopologyEntitiesRequest.newBuilder()
            .setTopologyType(TopologyType.SOURCE)
            .addAllEntityOids(entityIds)
            .setReturnType(PartialEntity.Type.ACTION)
            .setTopologyContextId(realtimeTopologyContextId)
            .build();
        return
            RepositoryDTOUtil.topologyEntityStream(repositoryService.retrieveTopologyEntities(getEntitiesrequest))
                .map(PartialEntity::getAction)
                .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
    }
}
