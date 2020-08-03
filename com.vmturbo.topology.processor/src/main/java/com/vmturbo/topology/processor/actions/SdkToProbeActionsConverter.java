package com.vmturbo.topology.processor.actions;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.MoveParameters;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class for converting Action Policies with SDK action types to
 * action policies with XL action types.
 */
public class SdkToProbeActionsConverter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * All the entities except storages.
     */
    private static final List<Integer> ENTITY_TYPES_BUT_STORAGE = Collections.unmodifiableList(
            EnumSet.allOf(EntityType.class)
                    .stream()
                    .filter(type -> EntityType.STORAGE != type)
                    .map(EntityType::getNumber)
                    .collect(Collectors.toList()));
    private static final Map<ActionItemDTO.ActionType, Supplier<ActionCapabilityElement.Builder>>
            CAPABILITY_CREATORS = createSdkToXlActionsMap();

    /**
     * Mapping of SDK action types to XL action types.
     *
     * Note that if multiple SDK action types map to the same XL action type the conflict must be resolved.
     * The conflict resolution is handled in Action Orchestrator ActionSupportResolver.
     * As of 3/9/2018, multiple conflicting action types are resolved by taking the MINIMUM support level.
     * So for example, if SUSPEND and TERMINATE both map to DEACTIVATE, and a probe specifies
     * NOT_SUPPORTED for SUSPEND and SUPPORTED for TERMINATE, then DEACTIVATE actions will be treated
     * as not supported for the probe.
     *
     * @return SDK-XL action types matches.
     */
    private static Map<ActionItemDTO.ActionType, Supplier<ActionCapabilityElement.Builder>> createSdkToXlActionsMap() {
        final Map<ActionItemDTO.ActionType, Supplier<ActionCapabilityElement.Builder>>
                capabilityBuilders = new EnumMap<>(ActionItemDTO.ActionType.class);
        capabilityBuilders.put(ActionItemDTO.ActionType.NONE,
                new SimpleCapabilityCreator(ActionDTO.ActionType.NONE));
        capabilityBuilders.put(ActionItemDTO.ActionType.START,
                new SimpleCapabilityCreator(ActionDTO.ActionType.ACTIVATE));
        capabilityBuilders.put(ActionItemDTO.ActionType.MOVE,
                new MoveCapabilityCreator(ENTITY_TYPES_BUT_STORAGE));
        capabilityBuilders.put(ActionItemDTO.ActionType.SCALE,
                new SimpleCapabilityCreator(ActionType.SCALE));
        capabilityBuilders.put(ActionItemDTO.ActionType.SUSPEND,
                new SimpleCapabilityCreator(ActionDTO.ActionType.DEACTIVATE));
        capabilityBuilders.put(ActionItemDTO.ActionType.TERMINATE,
                new SimpleCapabilityCreator(ActionDTO.ActionType.DEACTIVATE));
        capabilityBuilders.put(ActionItemDTO.ActionType.ADD_PROVIDER,
                new SimpleCapabilityCreator(ActionDTO.ActionType.START));
        capabilityBuilders.put(ActionItemDTO.ActionType.CHANGE,
                new MoveCapabilityCreator(Collections.singleton(EntityType.STORAGE.getNumber())));
        capabilityBuilders.put(ActionItemDTO.ActionType.PROVISION,
                new SimpleCapabilityCreator(ActionDTO.ActionType.PROVISION));
        capabilityBuilders.put(ActionItemDTO.ActionType.RECONFIGURE,
                new SimpleCapabilityCreator(ActionDTO.ActionType.RECONFIGURE));
        capabilityBuilders.put(ActionItemDTO.ActionType.RESIZE,
                new SimpleCapabilityCreator(ActionDTO.ActionType.RESIZE));
        capabilityBuilders.put(ActionItemDTO.ActionType.RESIZE_CAPACITY,
                new SimpleCapabilityCreator(ActionDTO.ActionType.RESIZE));
        capabilityBuilders.put(ActionItemDTO.ActionType.DELETE,
                new SimpleCapabilityCreator(ActionDTO.ActionType.DELETE));
        capabilityBuilders.put(ActionItemDTO.ActionType.RIGHT_SIZE,
                new SimpleCapabilityCreator(ActionDTO.ActionType.RESIZE));
        capabilityBuilders.put(ActionItemDTO.ActionType.CROSS_TARGET_MOVE,
                new SimpleCapabilityCreator(ActionDTO.ActionType.NONE));
        // Not sure that sdk MOVE_TOGETHER IS ACTUALLY MOVE
        capabilityBuilders.put(ActionItemDTO.ActionType.MOVE_TOGETHER,
                new SimpleCapabilityCreator(ActionDTO.ActionType.MOVE));
        return Collections.unmodifiableMap(capabilityBuilders);
    }

    /**
     * Utility class - hiding the constructor.
     */
    private SdkToProbeActionsConverter() {}

    /**
     * Converts all sdk policies in collection to xl policies.
     *
     * @param sdkPolicies Policies with sdk action types.
     * @return sdkPolicies converted to xl policies.
     */
    @Nonnull
    public static List<ProbeActionCapability> convert(@Nonnull Collection<ActionPolicyDTO> sdkPolicies) {
        return Objects.requireNonNull(sdkPolicies).stream()
                .map(SdkToProbeActionsConverter::convert).collect(Collectors.toList());
    }

    /**
     * Converts one sdk policy to xl policy.
     *
     * @param sdkActionPolicy sdk action type policy.
     * @return converted xl policy.
     */
    @Nonnull
    public static ProbeActionCapability convert(@Nonnull ActionPolicyDTO sdkActionPolicy) {
        ProbeActionCapability convertedPolicy = ProbeActionCapability.newBuilder()
                .setEntityType(Objects.requireNonNull(sdkActionPolicy)
                .getEntityType().getNumber())
                .addAllCapabilityElement(convertAllSdkPolicyElementsToXl(sdkActionPolicy
                        .getPolicyElementList())).build();
        logger.trace("{} was converted into {}", sdkActionPolicy, convertedPolicy);
        return convertedPolicy;
    }

    /**
     * Converts policy elements with sdk action type to xl policy elements.
     *
     * @param allPolicyElements policy elements to convert.
     * @return converted elements.
     */
    private static List<ProbeActionCapability.ActionCapabilityElement> convertAllSdkPolicyElementsToXl(
            @Nonnull List<ActionPolicyDTO.ActionPolicyElement> allPolicyElements) {

        return Objects.requireNonNull(allPolicyElements).stream()
                .map(SdkToProbeActionsConverter::convertSdkPolicyElementToXl)
                .collect(Collectors.toList());
    }

    /**
     * Converts policy element with sdk action type to xl policy element.
     *
     * @param sdkPolicyElement policy element to convert.
     * @return converted policy element.
     */
    private static ProbeActionCapability.ActionCapabilityElement convertSdkPolicyElementToXl(
            @Nonnull ActionPolicyDTO.ActionPolicyElement sdkPolicyElement) {
        Objects.requireNonNull(sdkPolicyElement);
        ActionCapabilityElement.Builder builder;
        if (sdkPolicyElement.getActionType() == null ||
                CAPABILITY_CREATORS.get(sdkPolicyElement.getActionType()) == null) {
            logger.warn("Failed to get action capability for action type: " +
                    sdkPolicyElement.getActionType());
            builder = CAPABILITY_CREATORS.get(ActionItemDTO.ActionType.NONE).get();
        } else {
            builder = CAPABILITY_CREATORS.get(sdkPolicyElement.getActionType()).get();
            if (builder.getActionType() == ActionType.NONE) {
                logger.debug(sdkPolicyElement.getActionType() + " was mapped into ActionType.NONE! " +
                        "Sdk policy element: " + sdkPolicyElement);
            }
        }
        builder.setActionCapability(convert(sdkPolicyElement.getActionCapability()));
        return builder.build();
    }

    @Nonnull
    private static ProbeActionCapability.ActionCapability convert(
            @Nonnull ActionPolicyDTO.ActionCapability src) {
        return Objects.requireNonNull(
                ProbeActionCapability.ActionCapability.forNumber(src.getNumber()));
    }

    /**
     * Simple capability creator is able to create builder for {@link ActionCapabilityElement} for
     * the specified entity type.
     */
    private static class SimpleCapabilityCreator implements
            Supplier<ActionCapabilityElement.Builder> {
        private final ActionDTO.ActionType actionType;

        private SimpleCapabilityCreator(ActionDTO.ActionType actionType) {
            this.actionType = Objects.requireNonNull(actionType);
        }

        @Override
        public ActionCapabilityElement.Builder get() {
            return ProbeActionCapability.ActionCapabilityElement.newBuilder()
                    .setActionType(actionType);
        }
    }

    /**
     * Move capability create creates a move action capability available for some specific entity
     * types.
     */
    private static class MoveCapabilityCreator extends SimpleCapabilityCreator {
        private final Collection<Integer> destinationTypesAllowed;

        private MoveCapabilityCreator(@Nonnull Collection<Integer> destinationTypesAllowed) {
            super(ActionType.MOVE);
            this.destinationTypesAllowed = Objects.requireNonNull(destinationTypesAllowed);
            if (destinationTypesAllowed.isEmpty()) {
                throw new IllegalArgumentException("destination types must not be empty");
            }
        }

        @Override
        public ActionCapabilityElement.Builder get() {
            return super.get()
                    .setMove(MoveParameters.newBuilder()
                            .addAllTargetEntityType(destinationTypesAllowed));
        }
    }
}
