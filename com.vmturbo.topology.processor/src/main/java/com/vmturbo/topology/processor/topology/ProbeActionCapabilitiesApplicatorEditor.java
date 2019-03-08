package com.vmturbo.topology.processor.topology;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider.Builder;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionPolicyElement;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;


/**
 * Editor to update following properties based on probes' action capabilities.
 * 1. movable:
 * a. When entity type is VM:
 * i. if provider is PM and action type is MOVE, set movable to true unless capability is set to NOT_SUPPORTED.
 * ii. if provider is Storage and action type is CHANGE, set movable to true unless capability is set to NOT_SUPPORTED.
 * iii. set movable to false for all other providers.
 * b. For all other entity types, set movable to true unless capability is set to NOT_SUPPORTED.
 * 2. cloneable:
 * cloneable is set to false, if there is no probe that can support the provision action
 * (all the probes/target for that entity have not supported for provision).
 * 3. suspendable:
 * suspendable is set to false, same as cloneable but for suspend action.
 * TODO: controllable
 * <p>
 * Special case when DTO is discovered by more than one probe.
 * a. If all probes don't support an action for an entity type, disable the action
 * for that entity (with same type).
 * b. If at least one probe doesn't have NOT_SUPPORT on an action for an entity type,
 * enable the action for that entity (with same type), except the action is disabled by user.
 * c. If at least one probe doesn't have action capabilities, handle the same ways as b.
 */
public class ProbeActionCapabilitiesApplicatorEditor {

    private static final Logger logger = LogManager.getLogger();
    // Does action capability set to NOT_SUPPORTED?
    private final static Predicate<ActionPolicyElement> IS_NOT_SUPPORTED =
        element -> element.getActionCapability() == ActionCapability.NOT_SUPPORTED;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;

    ProbeActionCapabilitiesApplicatorEditor(@Nonnull final ProbeStore probeStore,
                                            @Nonnull final TargetStore targetStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Update properties.
     *
     * @param graph The topology graph which contains all the SEs.
     */
    public EditorSummary applyPropertiesEdits(@Nonnull final TopologyGraph graph) {
        final Multimap<Long, UnsupportedAction> targetToProbeCapabilities =
            targetIdToEntityTypeMap(targetStore, probeStore);
        final EditorSummary editorSummary = new EditorSummary();
        graph.entities().forEach(entity -> {
                editMovable(graph, targetToProbeCapabilities, editorSummary, entity);
                editCloneable(targetToProbeCapabilities, editorSummary, entity);
                editSuspendable(targetToProbeCapabilities, editorSummary, entity);
            }
        );
        return editorSummary;
    }

    // edit cloneable
    private void editCloneable(@Nonnull final Multimap<Long, UnsupportedAction> targetToProbeCapabilities,
                               @Nonnull final EditorSummary editorSummary,
                               @Nonnull final TopologyEntity entity) {
        final AnalysisSettings.Builder builder = entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder();
        updateProperty(entity, targetToProbeCapabilities, ActionType.PROVISION,
            (isCloneable) -> {
                if (isCloneable) {
                    // at least one probe say it's cloneable, but if it's set already,
                    // e.g. user doesn't want to provision it, keep it as is.
                    if (!builder.hasCloneable()) {
                        builder.setCloneable(true);
                        editorSummary.increaseCloneableToTrueCount();
                    }
                } else { // probes all say "No" to "isCloneable", so set it to "false"
                    builder.setCloneable(false);
                    editorSummary.increaseCloneableToFalseCount();
                }
            });
    }

    // edit suspendable
    private void editSuspendable(@Nonnull final Multimap<Long, UnsupportedAction> targetToProbeCapabilities,
                                 @Nonnull final EditorSummary editorSummary,
                                 @Nonnull final TopologyEntity entity) {
        final AnalysisSettings.Builder builder = entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder();
        updateProperty(entity, targetToProbeCapabilities, ActionType.SUSPEND,
            (isSuspendable) -> {
                if (isSuspendable) {
                    // at least one probe say it's suspendable, but if it's set already,
                    // e.g. user doesn't want to suspend it,  keep it as is.
                    if (!builder.hasSuspendable()) {
                        builder.setSuspendable(true);
                        editorSummary.increaseSuspendableToTrueCount();
                    }
                } else { // probes all say "No" to "suspendable", so set it to "false"
                    builder.setSuspendable(false);
                    editorSummary.increaseSuspendableToFalseCount();
                }
            });
    }

    // edit movable
    private void editMovable(@Nonnull final TopologyGraph graph,
                             @Nonnull final Multimap<Long, UnsupportedAction> targetToProbeCapabilities,
                             @Nonnull final EditorSummary editorSummary,
                             @Nonnull final TopologyEntity entity) {
        entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList()
            .forEach(commoditiesBoughtFromProvider -> {
                    updateMovableBasedOnProbesCapabilities(commoditiesBoughtFromProvider, entity, graph,
                        targetToProbeCapabilities, editorSummary);
                }
            );
    }

    /**
     * Build a target capability map which store targetId -> (entityType, actionType).
     * The presence of the entry in the map would indicate the movable capability is not_supported.
     */
    private Multimap<Long, UnsupportedAction> targetIdToEntityTypeMap(@Nonnull final TargetStore targetStore,
                                                                      @Nonnull final ProbeStore probeStore) {
        final Multimap<Long, UnsupportedAction> targetIdToEntityTypeMap = ArrayListMultimap.create();
        targetStore.getAll().stream().forEach(target -> probeStore.getProbe(target.getProbeId()).ifPresent(
            probeInfo -> probeInfo.getActionPolicyList().stream()
                .filter(actionPolicyDTO -> actionPolicyDTO.hasEntityType())
                .forEach(
                    actionPolicyDTO -> actionPolicyDTO.getPolicyElementList().stream()
                        .filter(IS_NOT_SUPPORTED)
                        .forEach(actionPolicyElement -> targetIdToEntityTypeMap
                            .put(target.getId(), ImmutableUnsupportedAction.builder()
                                .entityType(actionPolicyDTO.getEntityType())
                                .actionType(actionPolicyElement.getActionType())
                                .build()))
                )));
        return targetIdToEntityTypeMap;
    }

    /**
     * 1. When entity type is VM:
     * a. if provider is PM and action type is MOVE, set movable to true unless capability is set to NOT_SUPPORTED
     * b. if provider is Storage and action type is CHANGE, set movable to true unless capability is set to NOT_SUPPORTED
     * c. set movable to false for all other providers
     * 2. For all other entity types, set movable to true unless capability is set to NOT_SUPPORTED
     *
     * @param builder                 commodityBoughtFromProviderBuilder builder
     * @param entity                  topology entity
     * @param graph                   topology graph
     * @param targetIdToEntityTypeMap targetId to entityType and actionType map. The presence of the
     *                                entry in the map would indicate the movable capability is not_supported.
     * @param editorSummary           object to keep track how many movables are set to true and false.
     */
    private void updateMovableBasedOnProbesCapabilities(@Nonnull final Builder builder,
                                                        @Nonnull final TopologyEntity entity,
                                                        @Nonnull final TopologyGraph graph,
                                                        @Nonnull final Multimap<Long, UnsupportedAction> targetIdToEntityTypeMap,
                                                        @Nonnull final EditorSummary editorSummary) {
        // Special case for VM
        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            final Optional<TopologyEntity> topologyEntity = graph.getEntity(builder.getProviderId());
            if (topologyEntity.isPresent()) {
                updateMovableForVM(builder, entity, targetIdToEntityTypeMap, topologyEntity.get(), editorSummary);
            } else {
                updateMovable(builder, true, editorSummary);
            }
        } else {
            updateProperty(entity, targetIdToEntityTypeMap, ActionType.MOVE,
                (isMovable) -> {
                    if (isMovable) {
                        // at least one probe say it's movable, but if it's set already,
                        // e.g. user doesn't want to move it, keep it as is.
                        if (!builder.hasMovable()) {
                            builder.setMovable(true);
                            editorSummary.increaseMovableToTrueCount();
                        }
                    } else { // probes all say "No" to "movable", so set it to "false"
                        builder.setMovable(false);
                        editorSummary.increaseMovableToFalseCount();
                    }
                }
            );
        }
    }

    /*
     * Update properties:
     * a. If all probes don't support an action for an entity type, disable the action
     * for that entity (with same type).
     * b. If at least one probe doesn't have NOT_SUPPORT on an action for an entity type,
     * enable the action for that entity (with same type), except the action is disabled by user.
     * c. If at least one probe doesn't have action capabilities, handle the same ways as b.
     */
    private void updateProperty(@Nonnull final TopologyEntity entity,
                                @Nonnull final Multimap<Long, UnsupportedAction> targetIdToEntityTypeMap,
                                @Nonnull final ActionType actionType,
                                @Nonnull final Consumer<Boolean> editPropertyConsumer) {
        final boolean hasAtLeastOneTargetSupportCapability = entity.getDiscoveringTargetIds().anyMatch(id -> {
                if (targetIdToEntityTypeMap.containsKey(id)) {
                    // if there exists an action policy doesn't support action with matched entityType
                    // then this target doesn't support this action.
                    return !targetIdToEntityTypeMap.get(id).stream()
                        .anyMatch(unsupportedAction ->
                            unsupportedAction.entityType().getNumber() == entity.getEntityType()
                                && unsupportedAction.actionType() == actionType);
                } else { // if this target didn't specific the action, the it can execute.
                    return true;
                }
            }
        );
        // If no target support this action, then setting the action to false
        if (!hasAtLeastOneTargetSupportCapability) {
            editPropertyConsumer.accept(false);
        } else {
            editPropertyConsumer.accept(true);
        }
    }

    // When entity type is VM:
    // a. if provider is PM and action type is MOVE, set movable to true unless capability is set to NOT_SUPPORTED
    // b. if provider is Storage and action type is CHANGE, set movable to true unless capability is set to NOT_SUPPORTED
    // c. set movable to false for all other providers
    private void updateMovableForVM(@Nonnull final Builder commodity,
                                    @Nonnull final TopologyEntity entity,
                                    @Nonnull final Multimap<Long, UnsupportedAction> targetIdToEntityTypeMap,
                                    @Nonnull final TopologyEntity provider,
                                    @Nonnull final EditorSummary movableEditSummary) {
        // for PM and Storage providers, check action capability
        if (provider.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
            updateMovable(commodity, entity, targetIdToEntityTypeMap, movableEditSummary, ActionType.MOVE);

        } else if (provider.getEntityType() == EntityType.STORAGE_VALUE) {
            updateMovable(commodity, entity, targetIdToEntityTypeMap, movableEditSummary, ActionType.CHANGE);
        } else {
            updateMovable(commodity, false, movableEditSummary);
        }
    }

    private void updateMovable(@Nonnull final Builder commodity,
                               @Nonnull final TopologyEntity entity,
                               @Nonnull final Multimap<Long, UnsupportedAction> targetIdToEntityTypeMap,
                               @Nonnull final EditorSummary movableEditSummary,
                               @Nonnull final ActionType actionType) {
        final boolean hasAtLeastOneTargetSupportCapability = entity.getDiscoveringTargetIds().anyMatch(id -> {
                if (targetIdToEntityTypeMap.containsKey(id)) {
                    // if there exists an action policy doesn't support action with matched entityType
                    // then this target doesn't support this action.
                    return !targetIdToEntityTypeMap.get(id).stream()
                        .anyMatch(unsupportedAction ->
                            unsupportedAction.entityType() == EntityType.VIRTUAL_MACHINE &&
                                unsupportedAction.actionType() == actionType);
                } else { // if this target didn't specific the action, the it can execute.
                    return true;
                }
            }
        );
        // If no target support move, then setting the "move" to false
        if (!hasAtLeastOneTargetSupportCapability) {
            updateMovable(commodity, false, movableEditSummary);
        } else {
            updateMovable(commodity, true, movableEditSummary);
        }
    }

    // update movable and add the change to summary
    private void updateMovable(@Nonnull final Builder commoditiesBoughtFromProvider,
                               final boolean isMovable,
                               @Nonnull final EditorSummary editorSummary) {
        commoditiesBoughtFromProvider.setMovable(isMovable);
        if (isMovable) {
            editorSummary.increaseMovableToTrueCount();
        } else {
            editorSummary.increaseMovableToFalseCount();
        }
    }

    // Value object to hold action capabilities for EntityType and ActionType in a target.
    @Value.Immutable
    interface UnsupportedAction {
        EntityType entityType();

        ActionType actionType();
    }

    public static class EditorSummary {
        // Integer.MAX_VALUE is around 2 billions. If XL needs to support more than 2 billions
        // commodities, we should upgrade the type. E.g. long type.
        private int movableToTrueCounter = 0;
        private int movableToFalseCounter = 0;
        private int cloneableToTrueCounter = 0;
        private int cloneableToFalseCounter = 0;
        private int suspendableToTrueCounter = 0;
        private int suspendableToFalseCounter = 0;

        public int getMovableToTrueCounter() {
            return movableToTrueCounter;
        }

        public int getMovableToFalseCounter() {
            return movableToFalseCounter;
        }

        public int getCloneableToTrueCounter() {
            return cloneableToTrueCounter;
        }

        public int getCloneableToFalseCounter() {
            return cloneableToFalseCounter;
        }

        public int getSuspendableToTrueCounter() {
            return suspendableToTrueCounter;
        }

        public int getSuspendableToFalseCounter() {
            return suspendableToFalseCounter;
        }

        private void increaseMovableToTrueCount() {
            ++movableToTrueCounter;
        }

        private void increaseMovableToFalseCount() {
            ++movableToFalseCounter;
        }

        private void increaseCloneableToTrueCount() {
            ++cloneableToTrueCounter;
        }

        private void increaseCloneableToFalseCount() {
            ++cloneableToFalseCounter;
        }

        private void increaseSuspendableToTrueCount() {
            ++suspendableToTrueCounter;
        }

        private void increaseSuspendableToFalseCount() {
            ++suspendableToFalseCounter;
        }
    }
}