package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionPolicyElement;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This editor updates Movable, Cloneable, and Suspendable properties of an entity based on probes'
 * action capabilities specified in ActionPolicyDTO.ActionCapability for an entity type.
 *
 * <p>The action capabilities here are used for market analysis, not for execution.
 *
 * <p>An entity may be discovered by more than one probe, the current resolution strategy is:
 * From the list of probes that discover the entity with proper action capabilities set,
 * - If all probes have specified NOT_SUPPORTED capability for the same entity and action type, then
 *   disable the action for this entity.
 * - Otherwise, enable the action for this entity, unless the action is disabled in the user policy
 *   settings. This includes the cases where:
 *   - the action capability is not set by the probe, then it is treated as NOT_EXECUTABLE
 *     (see the definition in ActionPolicyDTO protocol)
 *   - the action capability is set to either SUPPORTED, or NOT_EXECUTABLE
 *
 * <p>TODO: The above resolution strategy is not enough to cover all use cases, as it is performed
 *     at entity type level. In some cases, we want to handle resolution at individual entity
 *     level. For example, in a kubernetes cluster, even though in general ContainerPods are
 *     movable, there are certain pods that belong to DaemonSet which are deployed on every node
 *     in the cluster, and should NOT be movable.
 *     To support these use cases, we need to introduce action capability at entity level. During
 *     stitching, if an entity has entity level action capability from multiple probes, they need
 *     to be combined based on predefined rules.
 *     The final action capability of an entity should be determined from the following inputs:
 *     - Probe defined action capabilities at individual entity level (after stitching)
 *     - Probe defined action capabilities at the entity type level
 *     - User defined action policies
 *     - Default action policies
 *     - Other data in an entity including controllable, and overall state of an entity
 *
 * <p>TODO: Resize, Controllable
 *
 * <p>NOTE: This stage requires probes to provide correct action capabilities for specific entity
 * types. In particular, if an action type is not supported for an entity type, the probe must
 * specify NOT_SUPPORTED in the ActionCapability. Currently not all probes are doing this.
 *
 * <p>As a result, this stage is currently only enabled for entities coming from Cloud Native
 * targets, which do send correct action capabilities. For all other targets, we are still
 * using workaround in Market, e.g. in TopologyConverter#createShoppingList. It is expected
 * that once all probes provide proper action capabilities at both entity type and individual
 * entity level, we will enhance this stage to resolve action capabilities for all entities.
 */
public class ProbeActionCapabilitiesApplicatorEditor {
    // TARGET_PROBE_HAS_ACTION_CAPABILITIES is a predicate to determine if the probe that
    // a target is attached to has provided the required action capabilities in the
    // ActionPolicyDTO.ActionCapability protocol
    // A probe must specify correct action capabilities for specific entity types in order
    // for this editor to determine the correct action capabilities of entities discovered by
    // that probe. Currently,
    // - Cloud Native target probes are confirmed to provide these
    // information. Cloud Native targets have probe types prefixed with "Kubernetes", for example:
    // "Kubernetes-1848268059".
    // - AWS and AZURE Probes require the support for Move Volume Action capabilities.
    private static final Predicate<Target> TARGET_PROBE_HAS_ACTION_CAPABILITIES =
                target -> target.getProbeInfo().getProbeType().startsWith("Kubernetes") ||
                target.getProbeInfo().getProbeType().equals(SDKProbeType.AWS.getProbeType()) ||
                target.getProbeInfo().getProbeType().equals(SDKProbeType.AZURE.getProbeType());

    // In the future, when more probes start to provide proper capabilities, this predicate
    // can be modified to include those probes. When all probes start to provide proper
    // capabilities, this predicate can be removed
    // To enable for all probes -
//        private static final Predicate<Target> TARGET_PROBE_HAS_ACTION_CAPABILITIES =
//                target -> target.getProbeInfo().getActionPolicyCount() > 0;

    // IS_NOT_SUPPORTED_ACTION is a predicate to determine if an action can be enabled
    // for market analysis
    private static final Predicate<ActionPolicyElement> IS_NOT_SUPPORTED_ACTION =
            element -> element.getActionCapability() == ActionCapability.NOT_SUPPORTED;

    private final TargetStore targetStore;

    private static final Logger logger = LogManager.getLogger();

    ProbeActionCapabilitiesApplicatorEditor(@Nonnull final TargetStore targetStore) {
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Update properties.
     *
     * @param graph The topology graph which contains all the SEs.
     */
    public EditorSummary applyPropertiesEdits(@Nonnull final TopologyGraph<TopologyEntity> graph) {
        final Context context = new Context(targetStore);
        // Only edit action capabilities when there are at least one target probe in the system
        // that has proper action capabilities provided
        if (!context.targetsWithActionCapabilities.isEmpty()) {
            graph.entities().forEach(entity -> this.editActionCapabilities(entity, context));
        }
        return context.editorSummary;
    }

    private void editActionCapabilities(@Nonnull final TopologyEntity entity,
                                        @Nonnull final Context context) {
        List<Long> discoveryTargets =
                entity.getDiscoveringTargetIds()
                        .filter(context.targetsWithActionCapabilities::contains)
                        .collect(Collectors.toList());
        if (discoveryTargets.isEmpty()) {
            // None of the probes that discover this entity has action capabilities set
            return;
        }

        // At least one of the discovery targets should contain action capabilities metadata.
        if (!CollectionUtils.containsAny(context.entityTypesWithActionSettings.keySet(),
                                            discoveryTargets)) {
            return;
        }

        // Lists of probe action policies defined in the probe actions capabilities metadata
        // for this entity type
        EntityType entityType = EntityType.forNumber(entity.getEntityType());
        Stream<List<ProbeAction>> actionsList = context.entityTypesWithActionSettings.entrySet()
                                                .stream()
                                                .filter(e -> discoveryTargets.contains(e.getKey()))
                                                .map(e -> e.getValue())
                                                .map(m -> m.getOrDefault(entityType, Collections.emptyList()));

        // Map of action types and action policies specified by different targets for this entity type
        // Consolidated map of action types and associated action policies from different targets
        Map<ActionType, List<ProbeAction>> actionElementsMap = actionsList.flatMap(List::stream)
                .collect(Collectors.groupingBy(ProbeAction::actionType));

        // Did not find any defined action policies for any action types,
        // so the entity's analysis settings will not be modified.
        if (actionElementsMap.isEmpty()) {
            return;
        }

        editMovable(entity, actionElementsMap, context);
        editCloneable(entity, actionElementsMap, context);
        editSuspendable(entity, actionElementsMap, context);
    }

    // edit cloneable
    private void editCloneable(@Nonnull final TopologyEntity entity,
                               Map<ActionType, List<ProbeAction>> actionElementsMap,
                               @Nonnull final Context context) {
        final AnalysisSettings.Builder builder =
                entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder();

        // If the action is disabled at the entity level, do not override with probe's action policy
        if (builder.hasCloneable() && !builder.getCloneable()) {
            return;
        }

        // Change the analysis setting only if probe policies are found for the action type
        updateProperty(ActionType.PROVISION, actionElementsMap,
                (isCloneable) -> {
                    if (isCloneable) { // all probes with action policies say it is cloneable
                        if (!builder.hasCloneable()) {
                            builder.setCloneable(true);
                            context.editorSummary.increaseCloneableToTrueCount();
                        }
                    } else { // at least one of probes say "No" to "cloneable", so set it to "false"
                        builder.setCloneable(false);
                        context.editorSummary.increaseCloneableToFalseCount();
                        logger.trace("Disabled provision for {}::{}", entity.getEntityType(), entity.getDisplayName());
                    }
                });
    }

    // edit suspendable
    private void editSuspendable(@Nonnull final TopologyEntity entity,
                                 Map<ActionType, List<ProbeAction>> actionElementsMap,
                                 @Nonnull final Context context) {
        final AnalysisSettings.Builder builder =
                entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder();

        // If the action is disabled at the entity level, do not override with probe's action policy
        if (builder.hasSuspendable() && !builder.getSuspendable()) {
            return;
        }

        // Change the analysis setting only if probe policies are found for the action type
        updateProperty(ActionType.SUSPEND, actionElementsMap,
                (isSuspendable) -> {
                    if (isSuspendable) { // all probes with action policies say it is suspendable
                        // e.g. user doesn't want to suspend it,  keep it as is.
                        if (!builder.hasSuspendable()) {
                            builder.setSuspendable(true);
                            context.editorSummary.increaseSuspendableToTrueCount();
                        }
                    } else { // at least one of probes say "No" to "suspendable", so set it to "false"
                        builder.setSuspendable(false);
                        context.editorSummary.increaseSuspendableToFalseCount();
                        logger.trace("Disabled suspendable for {}::{}", entity.getEntityType(), entity.getDisplayName());
                    }
                });
    }

    // edit movable
    private void editMovable(@Nonnull final TopologyEntity entity,
                             Map<ActionType, List<ProbeAction>> actionElementsMap,
                             @Nonnull final Context context) {

        // If VV's discoveryTarget(s) all indicated movable NOT_SUPPORTED,
        // The storage tier commodities of the VM, which VV is attached to, is set to movable false.
        // This will not get overwritten by the VM enabling movable action setting since the existing
        // logic only enable it if it hasn't been set.
        if (EntityType.VIRTUAL_VOLUME_VALUE == entity.getEntityType()) {
            // Here we try NOT to make the assumption that the target which discovered the VV and VM are the same.
            // Even though we mark the commodity of VM as non-movable, the probe takes movable action against VV, not VM.
            // Therefore the checking should be against vv's discovery targets
            final boolean vvNotMovable = actionElementsMap.getOrDefault(ActionType.MOVE, Arrays.asList()).stream()
                                                         .anyMatch(a -> a.actionSetting() == false);

            if (vvNotMovable) {
                entity.getInboundAssociatedEntities().stream()
                    .filter(associatedEntity -> associatedEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                    .forEach(vmEntity -> vmEntity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList().stream()
                        .filter(CommoditiesBoughtFromProvider.Builder::hasProviderId)
                        .filter(CommoditiesBoughtFromProvider.Builder::hasProviderEntityType)
                        .filter(builder -> builder.getProviderEntityType() == EntityType.STORAGE_TIER_VALUE)
                        .filter(builder -> builder.getVolumeId() == entity.getOid())
                        .forEach(builder -> {
                            builder.setMovable(false);
                            context.editorSummary.increaseMovableToFalseCount();
                        })
                    );
            }
        }
        entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList()
                .forEach(builder ->
                        updateProperty(ActionType.MOVE, actionElementsMap,
                                (isMovable) -> {
                                    // If the action is disabled at the entity level,
                                    // do not override with probe's action policy
                                    if (builder.hasMovable() && !builder.getMovable()) {
                                        return;
                                    }
                                    if (isMovable) {
                                        // all probes with action policies say it is movable,
                                        if (!builder.hasMovable()) {
                                            builder.setMovable(true);
                                            context.editorSummary.increaseMovableToTrueCount();
                                        }
                                    } else { // at least one of probes says "No" to "movable", so set it to "false"
                                        builder.setMovable(false);
                                        context.editorSummary.increaseMovableToFalseCount();
                                        logger.trace("Disabled movable for {}::{} on provider {}",
                                                    entity.getEntityType(), entity.getDisplayName(),
                                                    builder.getProviderEntityType());
                                    }
                                }
                        ));
    }

    /*
     * Update action capabilities for an entity.
     *
     * From the list of probes that discover this entity with proper action capabilities set:
     * a. If there are no probe policies found for the entity and action type, then do not
     *    change the analysis setting for the entity.
     * b. If any of probes have specified NOT_SUPPORTED for the same entity and action type, then
     *    disable the action for this entity.
     * c. If all of probes have specified SUPPORTED or EXECUTABLE for the same entity and action type,
     *    then enable the action for this entity,
     */
    private void updateProperty(@Nonnull final ActionType actionType,
                                @Nonnull Map<ActionType, List<ProbeAction>> actionElementsMap,
                                @Nonnull final Consumer<Boolean> editPropertyConsumer) {

        // no policies are found in any of the associated probes for this action type
        // do not update property
        if (!actionElementsMap.containsKey(actionType) || actionElementsMap.get(actionType).isEmpty()) {
            return;
        }

        Collection<ProbeAction> actionElements = actionElementsMap.get(actionType);

        // found policies
        if (actionElements.stream().anyMatch(action -> action.actionSetting() == false)) {
            // Disable the action setting in the entity
            editPropertyConsumer.accept(false);
        } else {
            // Enable the action setting in the entity
            editPropertyConsumer.accept(true);
        }
    }

    // Value object to hold action capabilities for EntityType and ActionType in a target.
    @Value.Immutable
    interface ProbeAction {
        EntityType entityType();

        ActionType actionType();

        boolean actionSetting();
    }

    private static class Context {
        // targetsWithActionCapabilities stores a list of targets whose probes have proper action
        // capabilities set as determined by the TARGET_PROBE_HAS_ACTION_CAPABILITIES predicate
        private final List<Long> targetsWithActionCapabilities = new ArrayList<>();

        // unsupportedActions stores unsupported actions (as determined by the IS_NOT_SUPPORTED_ACTION
        // predicate) based on target ID: targetId -> (entityType, actionType)
        private final Multimap<Long, ProbeAction> unsupportedActions = ArrayListMultimap.create();

        // Map of target Id and the associated action policies for different entity types belonging to the target
        private Map<Long, Map<EntityType, List<ProbeAction>>> entityTypesWithActionSettings = new HashMap<>();

        private final EditorSummary editorSummary = new EditorSummary();
        private final TargetStore targetStore;

        private Context(@Nonnull final TargetStore targetStore) {
            this.targetStore = targetStore;
            cacheTargetsAndProbeActionCapabilities();
        }

        private ProbeAction createProbeAction(ActionPolicyDTO actionPolicyDTO,
                                               ActionPolicyElement actionPolicyElement) {

            boolean actionSetting = false;
            if (actionPolicyElement.getActionCapability() == ActionCapability.SUPPORTED ||
                    actionPolicyElement.getActionCapability() == ActionCapability.NOT_EXECUTABLE) {
                actionSetting = true;
            } else if (actionPolicyElement.getActionCapability() == ActionCapability.NOT_SUPPORTED) {
                actionSetting = false;
            }
            return ImmutableProbeAction.builder()
                    .actionType(actionPolicyElement.getActionType())
                    .entityType(actionPolicyDTO.getEntityType())
                    .actionSetting(actionSetting)
                    .build();
        }

        /**
         * Build a probe action capability map by target which store targetId -> (entityType, actionType).
         */
        private void cacheTargetsAndProbeActionCapabilities() {

            List<ActionType> editableActions = Arrays.asList(ActionType.MOVE,
                                                                ActionType.SUSPEND,
                                                                ActionType.PROVISION);
            entityTypesWithActionSettings =
                    targetStore.getAll().stream()
                    .filter(TARGET_PROBE_HAS_ACTION_CAPABILITIES)
                    .collect(Collectors.toMap(
                            target -> target.getId(),
                            target -> target.getProbeInfo().getActionPolicyList().stream()
                                    .filter(ActionPolicyDTO::hasEntityType)
                                    .collect(Collectors.toMap(
                                            actionPolicyDTO -> actionPolicyDTO.getEntityType(),
                                            actionPolicyDTO -> actionPolicyDTO.getPolicyElementList().stream()
                                                    .filter(e -> editableActions.contains(e.getActionType()))
                                                    .map(e -> createProbeAction(actionPolicyDTO, e))
                                                    .collect(Collectors.toList())))
                    ));

            targetStore.getAll().stream().filter(TARGET_PROBE_HAS_ACTION_CAPABILITIES)
                    .forEach(target -> {
                                targetsWithActionCapabilities.add(target.getId());
                                target.getProbeInfo().getActionPolicyList().stream()
                                        .filter(ActionPolicyDTO::hasEntityType)
                                        .forEach(actionPolicyDTO ->
                                                actionPolicyDTO.getPolicyElementList().stream()
                                                        .filter(IS_NOT_SUPPORTED_ACTION)
                                                        .forEach(e -> {
                                                            unsupportedActions
                                                                .put(target.getId(),
                                                                        createProbeAction(actionPolicyDTO, e));
                                                        }));
                            }
                    );

            logger.trace("actionsWithSettings: {}", entityTypesWithActionSettings);
            logger.trace("unsupportedActions: {}", unsupportedActions);
            logger.trace("targetsWithActionCapabilities: {}", targetsWithActionCapabilities);
        }
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
