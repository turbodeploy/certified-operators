package com.vmturbo.topology.processor.actions;

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;

/**
 * Class for converting Action Policies with SDK action types to
 * action policies with XL action types.
 */
public class SdkToProbeActionsConverter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * SDK-XL Action types matches.
     */
    private static final Map<ActionItemDTO.ActionType, ActionDTO.ActionType> SDK_TO_XL_ACTIONS =
            createSdkToXlActionsMap();

    /**
     * Mapping of SDK action types to XL action types.
     *
     * @return SDK-XL action types matches.
     */
    private static Map<ActionItemDTO.ActionType, ActionDTO.ActionType> createSdkToXlActionsMap() {
        @SuppressWarnings("unchecked")
        Map<ActionItemDTO.ActionType, ActionDTO.ActionType> sdkToXlActions =
                new EnumMap(ActionItemDTO.ActionType.class);
        sdkToXlActions.put(ActionItemDTO.ActionType.NONE, ActionDTO.ActionType.NONE);
        sdkToXlActions.put(ActionItemDTO.ActionType.START, ActionDTO.ActionType.ACTIVATE);
        sdkToXlActions.put(ActionItemDTO.ActionType.MOVE, ActionDTO.ActionType.MOVE);
        sdkToXlActions.put(ActionItemDTO.ActionType.SUSPEND, ActionDTO.ActionType.DEACTIVATE);
        sdkToXlActions.put(ActionItemDTO.ActionType.TERMINATE, ActionDTO.ActionType.DEACTIVATE);
        sdkToXlActions.put(ActionItemDTO.ActionType.SPAWN, ActionDTO.ActionType.NONE);
        sdkToXlActions.put(ActionItemDTO.ActionType.ADD_PROVIDER, ActionDTO.ActionType.START);
        sdkToXlActions.put(ActionItemDTO.ActionType.CHANGE, ActionDTO.ActionType.MOVE);
        sdkToXlActions.put(ActionItemDTO.ActionType.REMOVE_PROVIDER, ActionDTO.ActionType
                .DEACTIVATE);
        sdkToXlActions.put(ActionItemDTO.ActionType.PROVISION, ActionDTO.ActionType.PROVISION);
        sdkToXlActions.put(ActionItemDTO.ActionType.RECONFIGURE, ActionDTO.ActionType.RECONFIGURE);
        sdkToXlActions.put(ActionItemDTO.ActionType.RESIZE, ActionDTO.ActionType.RESIZE);
        sdkToXlActions.put(ActionItemDTO.ActionType.RESIZE_CAPACITY, ActionDTO.ActionType.RESIZE);
        sdkToXlActions.put(ActionItemDTO.ActionType.WARN, ActionDTO.ActionType.NONE);
        sdkToXlActions.put(ActionItemDTO.ActionType.RECONFIGURE_THRESHOLD, ActionDTO.ActionType
                .RECONFIGURE);
        sdkToXlActions.put(ActionItemDTO.ActionType.DELETE, ActionDTO.ActionType.NONE);
        sdkToXlActions.put(ActionItemDTO.ActionType.RIGHT_SIZE, ActionDTO.ActionType.RESIZE);
        sdkToXlActions.put(ActionItemDTO.ActionType.RESERVE_ON_PM, ActionDTO.ActionType.NONE);
        sdkToXlActions.put(ActionItemDTO.ActionType.RESERVE_ON_DS, ActionDTO.ActionType.NONE);
        sdkToXlActions.put(ActionItemDTO.ActionType.RESIZE_FOR_EFFICIENCY, ActionDTO.ActionType
                .RESIZE);
        sdkToXlActions.put(ActionItemDTO.ActionType.CROSS_TARGET_MOVE, ActionDTO.ActionType.NONE);
        sdkToXlActions.put(ActionItemDTO.ActionType.RESERVE_ON_DA, ActionDTO.ActionType.NONE);
        // Not sure that sdk MOVE_TOGETHER IS ACTUALLY MOVE
        sdkToXlActions.put(ActionItemDTO.ActionType.MOVE_TOGETHER, ActionDTO.ActionType.MOVE);
        return sdkToXlActions;
    }

    /**
     * Converts all sdk policies in collection to xl policies.
     *
     * @param sdkPolicies Policies with sdk action types.
     * @return sdkPolicies converted to xl policies.
     */
    @Nonnull
    public static List<ProbeActionCapability> convert(@Nonnull List<ActionPolicyDTO> sdkPolicies) {
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

        ActionDTO.ActionType mapedType = SDK_TO_XL_ACTIONS.get(sdkPolicyElement.getActionType());
        if (mapedType == ActionType.NONE) {
            logger.warn(sdkPolicyElement.getActionType() + " was maped into ActionType.NONE! " +
                    "Sdk policy element: " + sdkPolicyElement);
        }
        return ProbeActionCapability.ActionCapabilityElement
                .newBuilder()
                .setActionCapability(ProbeActionCapability.ActionCapability
                .forNumber(Objects.requireNonNull(sdkPolicyElement)
                .getActionCapability().getNumber()))
                .setActionType(mapedType)
                .build();
    }
}
