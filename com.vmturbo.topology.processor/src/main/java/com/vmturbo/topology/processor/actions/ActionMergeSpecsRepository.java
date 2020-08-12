package com.vmturbo.topology.processor.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionEntity;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.MoveMergeSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ProvisionMergeSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ResizeMergeSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ScaleMergeSpec;
import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.platform.common.dto.ActionExecution.ActionMergeExecutionTarget;
import com.vmturbo.platform.common.dto.ActionExecution.ActionMergePolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionMergePolicyDTO.ActionSpecCase;
import com.vmturbo.platform.common.dto.ActionExecution.ActionMergeTargetData;
import com.vmturbo.platform.common.dto.ActionExecution.ChainedActionMergeTargetData;
import com.vmturbo.platform.common.dto.ActionExecution.ChainedActionMergeTargetData.TargetDataLink;
import com.vmturbo.platform.common.dto.CommonDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsRepository.ActionMergeSpecsBuilder.ActionExecutionTarget;

/**
 * The {@link ActionMergeSpecsRepository} implements the action merge and conversion repository.
 * The idea is to maintain the current snapshot as provided by the probes.
 */
public class ActionMergeSpecsRepository {

    private final Logger logger = LogManager.getLogger();

    // probe to policies
    private final Map<Long, Map<EntityDTO.EntityType, ActionMergePolicyDTO>> actionMergePolicyMap = new HashMap<>();

    /**
     * Save the action merge policies sent by the probe during registration.
     *
     * @param probeId   Probe Id
     * @param probeInfo {@link ProbeInfo} containing the list of {@link ActionMergePolicyDTO}'s
     */
    public void setPoliciesForProbe(long probeId, @Nonnull final ProbeInfo probeInfo) {
        if (probeInfo.getActionMergePolicyCount() >= 0) {
            logger.debug("received action merge policies from probe {}",
                                probeInfo.getActionMergePolicyCount(), probeId);

            Map<EntityDTO.EntityType, ActionMergePolicyDTO> policyMap
                    = probeInfo.getActionMergePolicyList().stream()
                        .filter(policy -> policy.getExecutionTargetsCount() > 0)   //filter out policies without target data
                        .filter(policy -> policy.hasResizeSpec()
                                            || policy.hasMoveSpec()
                                            || policy.hasProvisionSpec()
                                            || policy.hasScaleSpec())
                        .collect(Collectors.toMap(ActionMergePolicyDTO::getEntityType, Function.identity()));

            logger.trace("probe {} has action merge policies for {}", probeId, policyMap.keySet());
            actionMergePolicyMap.put(probeId, policyMap);
        }
    }

    /**
     * Create a list of {@link AtomicActionSpec} for the entities belonging
     * to a given probe and target.
     *
     * @param probeId   Probe Id
     * @param targetId      Target Id
     * @param topologyGraph {@link TopologyGraph} graph of entities with connections after stitching
     *
     * @return  list of {@link AtomicActionSpec} for the entities
     */
    public List<AtomicActionSpec> createAtomicActionSpecs(Long probeId, Long targetId,
                                                          TopologyGraph<TopologyEntity> topologyGraph) {
        // no action merge policies for the given probe, nothing to do
        if (!actionMergePolicyMap.containsKey(probeId)) {
            // this unexpected since the upload stage should use probe which contain the policy DTOs
            logger.warn("{} : cannot find action merge policy dtos");
            return new ArrayList<>();
        }
        // Map of policies by entity type
        Map<EntityType, ActionMergePolicyDTO> policyMap = actionMergePolicyMap.get(probeId);

        Map<Integer, List<TopologyEntity>> discoveredEntityList =
        policyMap.keySet().stream()
                .flatMap(entityType -> topologyGraph.entitiesOfType(entityType.getNumber())
                                    .filter(entity -> entity.getDiscoveringTargetIds()
                                                .anyMatch(discoveringTarget -> targetId.equals(discoveringTarget))))
                .collect(Collectors.groupingBy(TopologyEntity::getEntityType));

        ActionMergeSpecsBuilder actionMergeSpecBuilder
                = new ActionMergeSpecsBuilder(targetId, topologyGraph);

        for (EntityType policyEntityType : policyMap.keySet()) {
            // no entities of this type were discovered, nothing to do
            if (!discoveredEntityList.containsKey(policyEntityType.getNumber())) {
                logger.warn("{} : no discovered entities for entity type {}", targetId, policyEntityType);
                continue;
            }

            List<TopologyEntity> entities = discoveredEntityList.get(policyEntityType.getNumber());
            if (entities.size() == 0) {
                logger.warn("{} : no discovered entities for entity type {}", targetId, policyEntityType);
                continue;
            }

            // Policy for this entity type
            ActionMergePolicyDTO mergePolicyDTO = policyMap.get(policyEntityType);

            // Iterate over all the entities of this type to create or find the action merge spec
            // based on the aggregate entity it is connected to
            for (TopologyEntity policyEntity : entities) {
                actionMergeSpecBuilder.entityActionMergeSpec(policyEntity, mergePolicyDTO);
            }
        } //done all entity types

        return actionMergeSpecBuilder.getAtomicActionSpecs();
    }

    /**
     * Builder to create action merge specs for entities belonging to a target.
     */
    static class ActionMergeSpecsBuilder {

        private final Logger logger = LogManager.getLogger();
        private final TopologyGraph<TopologyEntity> topologyGraph;
        private final Long targetId;

        // AtomicActionSpec.Builder by aggregate and de-duplication entity
        private Map<TopologyEntity, Map<Optional<TopologyEntity>, AtomicActionSpec.Builder>> atomicActionSpecsMap;

        /**
         * Constructor.
         *
         * @param targetId Target id of the entities for which the spec is being created
         * @param topologyGraph {@link TopologyGraph}
         */
        ActionMergeSpecsBuilder(Long targetId, TopologyGraph<TopologyEntity> topologyGraph) {
            this.targetId = targetId;
            this.topologyGraph = topologyGraph;
            this.atomicActionSpecsMap = new HashMap<>();
        }

        /**
         * Returns list of atomic action specs created for target entities.
         *
         * @return list of {@link AtomicActionSpec}
         */
        public List<AtomicActionSpec> getAtomicActionSpecs() {
            logger.trace("Target {} : created atomic action specs for aggregate entities {}",
                                    targetId, atomicActionSpecsMap.keySet());
            for (TopologyEntity e1 : atomicActionSpecsMap.keySet()) {
                logger.trace("Target {} : created {} atomic action specs for aggregate entity {} "
                                + "that will be deduplicated to {}",
                        targetId,
                        atomicActionSpecsMap.get(e1).size(), e1,
                        atomicActionSpecsMap.get(e1).keySet());
            }

            List<AtomicActionSpec> result = atomicActionSpecsMap.values().stream()
                    .flatMap(item -> item.values().stream().map(builder -> builder.build()))
                    .collect(Collectors.toList());

            logger.debug("Target {} : total atomic action specs {}", targetId, result.size());
            return result;
        }

        /**
         * Create new or associate a {@link AtomicActionSpec} for the given entity.
         *
         * @param entity   {@link TopologyEntity} for which the action merge spec will be generated
         * @param mergePolicyDTO {@link ActionMergePolicyDTO} associated with the entity
         * @return
         */
        public void entityActionMergeSpec(TopologyEntity entity,
                                          ActionMergePolicyDTO mergePolicyDTO) {

            //  get the action execution entity
            ActionExecutionTarget executionTarget = getActionExecutionTarget(entity, mergePolicyDTO);
            if (executionTarget == null) {
                // The search for the execution target will fail for entities such as
                // bare pods, which will match criteria for action merge,
                // but they won't be connected to any ContainerSpecs or WorkloadControllers
                return;
            }
            TopologyEntity aggregationEntity = executionTarget.actionAggregationEntity();
            Optional<TopologyEntity> deDuplicationEntityOpt = executionTarget.actionDeDuplicationEntity();

            Map<Optional<TopologyEntity>, AtomicActionSpec.Builder> specsMap
                    = atomicActionSpecsMap.computeIfAbsent(aggregationEntity, v -> new HashMap<>());

            if (!specsMap.containsKey(deDuplicationEntityOpt)) {
                // new spec with execution target
                AtomicActionSpec.Builder mergeSpec = AtomicActionSpec.newBuilder();
                mergeSpec.setActionMergePolicy(mergePolicyDTO);
                mergeSpec.setAggregateEntity(
                                AtomicActionEntity.newBuilder()
                                        .setEntity(ActionEntity.newBuilder()
                                                .setType(aggregationEntity.getEntityType())
                                                .setId(aggregationEntity.getOid())
                                                .setEnvironmentType(aggregationEntity.getEnvironmentType())
                                                .build())
                                        .setEntityName(aggregationEntity.getDisplayName()));


                // Need to add action info
                ActionSpecSetter specSetter = new ActionSpecSetter(mergePolicyDTO, executionTarget);
                specSetter.setActionSpec(mergeSpec);

                specsMap.put(deDuplicationEntityOpt, mergeSpec);

                atomicActionSpecsMap.put(aggregationEntity, specsMap);
            }
            // add current entity OID in the spec
            AtomicActionSpec.Builder mergeSpec = specsMap.get(deDuplicationEntityOpt);
            mergeSpec.addEntityIds(entity.getOid());
        }

        /**
         * Value to hold the execution target entity for the merged actions for a set of entities.
         */
        @Value.Immutable
        interface ActionExecutionTarget {
            TopologyEntity actionAggregationEntity();

            Optional<TopologyEntity> actionDeDuplicationEntity();
        }

        /**
         * Find the execution target entity which will execute the actions of the given entity
         * when they are merged based on the given action merge policy DTO.
         * The list of {@link ActionMergeExecutionTarget} in the policy is iterated over
         * in the order provided until the execution targets is found.
         * If no ActionMergeExecutionTarget is found from the list, action merge will not be
         * performed for the entity.
         *
         * @param entity            {@link TopologyEntity} entity whose actions will be merged
         * @param mergePolicyDTO    @{link ActionMergePolicyDTO} action merge policy DTO.
         *
         * @return {@link ActionMergeExecutionTarget} if one is found, else null
         */
        @VisibleForTesting
        ActionExecutionTarget getActionExecutionTarget(TopologyEntity entity,
                                                        ActionMergePolicyDTO mergePolicyDTO) {
            for (ActionMergeExecutionTarget targetData : mergePolicyDTO.getExecutionTargetsList()) {
                ActionExecutionTarget executionTargetEntity = null;

                if (targetData.hasChainedMergeTarget()) {
                    executionTargetEntity
                            = getChainConnectedEntity(entity, targetData.getChainedMergeTarget());
                } else if (targetData.hasMergeTarget()) {
                    Optional<TopologyEntity> connectedEntity
                            = getConnectedEntity(entity, targetData.getMergeTarget());
                    if (connectedEntity.isPresent()) {
                        executionTargetEntity = ImmutableActionExecutionTarget.builder()
                                                .actionAggregationEntity(connectedEntity.get())
                                                .build();
                    }
                }

                // If we find the execution target entity, return it
                if (executionTargetEntity != null) {
                    return executionTargetEntity;
                }
            }

            return null;
        }

        private ActionExecutionTarget getChainConnectedEntity(TopologyEntity entity,
                                                       ChainedActionMergeTargetData chainedTarget) {
            Optional<TopologyEntity> deDuplicationEntity = Optional.empty();
            Optional<TopologyEntity> connectedEntity = Optional.empty();

            TopologyEntity startingEntity = entity;
            for (TargetDataLink targetDataLink : chainedTarget.getTargetLinksList()) {
                ActionMergeTargetData targetData = targetDataLink.getMergeTarget();
                connectedEntity = getConnectedEntity(startingEntity, targetData);

                // if the connected entity at this stage is not found, no need to proceed further
                if (!connectedEntity.isPresent()) {
                    break;
                }
                // save this entity as de-duplication target if this link is marked as such
                if (targetDataLink.getDeDuplicate()) {
                    deDuplicationEntity = connectedEntity;
                }
                startingEntity = connectedEntity.get();
            }

            if (!connectedEntity.isPresent()) {
                return null;
            }

            return ImmutableActionExecutionTarget.builder()
                        .actionAggregationEntity(connectedEntity.get())
                        .actionDeDuplicationEntity(deDuplicationEntity)
                        .build();
        }

        private Optional<TopologyEntity> getConnectedEntity(TopologyEntity entity,
                                        ActionMergeTargetData targetData) {

            // Connection is not specified, the entity will act as the target for merged actions
            if (!targetData.hasRelatedBy()) {
                return Optional.of(entity);
            }
            ConnectionType connection = targetData.getRelatedBy().getConnectionType();

            Stream<TopologyEntity> connectedEntities;
            if (connection == ConnectionType.AGGREGATED_BY_CONNECTION) {
                connectedEntities = topologyGraph.getAggregators(entity);
            } else if (connection == ConnectionType.CONTROLLED_BY_CONNECTION) {
                connectedEntities = topologyGraph.getControllers(entity);
            } else if (connection == ConnectionType.OWNS_CONNECTION) {
                connectedEntities = topologyGraph.getOwner(entity);
            } else {
                return Optional.empty();
            }

            // TODO: log warning if multiple connected entities are found
            EntityType relatedEntityType = targetData.getRelatedTo();
            Optional<TopologyEntity> connectedEntity = connectedEntities
                                                .filter(e -> e.getEntityType()
                                                                == relatedEntityType.getNumber())
                                                .findFirst();
            return connectedEntity;
        }
    }

    /**
     * Class to set the action specs based
     * on different action types in the {@link AtomicActionSpec.Builder}.
     */
    static class ActionSpecSetter {

        private final ActionExecutionTarget executionTarget;
        private final ActionMergePolicyDTO mergePolicyDTO;

        ActionSpecSetter(@Nonnull ActionMergePolicyDTO mergePolicyDTO,
                         @Nonnull ActionExecutionTarget executionTarget) {
            this.executionTarget = executionTarget;
            this.mergePolicyDTO = mergePolicyDTO;
        }

        /**
         * Set the action spec based on {@link ActionSpecCase}.
         * @param builder {@link AtomicActionSpec.Builder} where the action spec is filled in
         */
        void setActionSpec(AtomicActionSpec.Builder builder) {
            switch (mergePolicyDTO.getActionSpecCase()) {
                case MOVESPEC:
                    setMoveSpec(builder);
                    return;
                case PROVISIONSPEC:
                    setProvisionSpec(builder);
                    return;
                case RESIZESPEC:
                    setResizeSpec(builder);
                    return;
                case SCALESPEC:
                    setScaleSpec(builder);
                    return;
                default:
                    return;
            }
        }

        /**
         * Set the move action spec.
         * @param builder builder {@link AtomicActionSpec.Builder} where the move action spec is set
         */
        private void setMoveSpec(AtomicActionSpec.Builder builder) {
            builder.setMoveSpec(MoveMergeSpec.newBuilder()
                    .setProviderType(mergePolicyDTO.getMoveSpec().getProviderType())
                    .build());
        }

        /**
         * Set the provision action spec.
         * @param builder builder {@link AtomicActionSpec.Builder} where the provision action spec is set
         */
        private void setProvisionSpec(AtomicActionSpec.Builder builder) {
            builder.setProvisionSpec(ProvisionMergeSpec.newBuilder()
                    .build());
        }

        /**
         * Set the resize action spec.
         * @param builder builder {@link AtomicActionSpec.Builder} where the resize action spec is set
         */
        private void setResizeSpec(AtomicActionSpec.Builder builder) {

            List<ResizeMergeSpec.CommodityMergeData> commDataList = new ArrayList<>();

            for (ActionExecution.ResizeMergeSpec.CommodityMergeData commData
                    : mergePolicyDTO.getResizeSpec().getCommodityDataList()) {
                commDataList.add(ResizeMergeSpec.CommodityMergeData.newBuilder()
                        .setCommodityType(commData.getCommodityType())
                        .setChangedAttr(commData.getChangedAttr())
                        .build()
                );
            }

            ResizeMergeSpec.Builder resizeSpecBuilder = ResizeMergeSpec.newBuilder()
                    .addAllCommodityData(commDataList);

            executionTarget.actionDeDuplicationEntity().ifPresent(deDuplicationEntity -> {
                resizeSpecBuilder.setDeDuplicationTarget(
                        AtomicActionEntity.newBuilder()
                                .setEntity(ActionEntity.newBuilder()
                                        .setId(deDuplicationEntity.getOid())
                                        .setType(deDuplicationEntity.getEntityType())
                                        .setEnvironmentType(deDuplicationEntity.getEnvironmentType())
                                        .build())
                                .setEntityName(deDuplicationEntity.getDisplayName())
                                .build());
            });

            builder.setResizeSpec(resizeSpecBuilder.build());
        }

        /**
         * Set the scale action spec.
         * @param builder builder {@link AtomicActionSpec.Builder} where the scale action spec is set
         */
        private void setScaleSpec(AtomicActionSpec.Builder builder) {
            List<ScaleMergeSpec.CommodityMergeData> commDataList = new ArrayList<>();

            for (ActionExecution.ScaleMergeSpec.CommodityMergeData commData
                    : mergePolicyDTO.getScaleSpec().getCommodityDataList()) {
                commDataList.add(ScaleMergeSpec.CommodityMergeData.newBuilder()
                        .setCommodityType(commData.getCommodityType())
                        .setChangedAttr(commData.getChangedAttr())
                        .build()
                );
            }

            ScaleMergeSpec.Builder scaleSpecBuilder = ScaleMergeSpec.newBuilder()
                    .addAllCommodityData(commDataList);

            executionTarget.actionDeDuplicationEntity().ifPresent(deDuplicationEntity -> {
                scaleSpecBuilder.setDeDuplicationTarget(
                        AtomicActionEntity.newBuilder()
                                .setEntity(ActionEntity.newBuilder()
                                        .setId(deDuplicationEntity.getOid())
                                        .setType(deDuplicationEntity.getEntityType())
                                        .setEnvironmentType(deDuplicationEntity.getEnvironmentType())
                                        .build())
                                .setEntityName(deDuplicationEntity.getDisplayName())
                                .build());
            });

            builder.setScaleSpec(scaleSpecBuilder.build());
        }
    }
}
