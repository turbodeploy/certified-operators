package com.vmturbo.api.conversion.action;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.conversion.entity.EntityTypeMapping;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionCharacteristicApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.ActionDisruptiveness;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionReversibility;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.SDKUtil;

/**
 * This provides information about action for conversion based on an {@code ActionExecutionDTO}
 * object.
 */
public class SdkActionInformationProvider implements ActionInformationProvider {
    private static final String TARGET_ADDRESS = "targetAddress";
    private static final String FORMAT_FOR_ACTION_VALUES = "%.1f";

    private static final String API_CATEGORY_PERFORMANCE_ASSURANCE = "Performance Assurance";
    private static final String API_CATEGORY_EFFICIENCY_IMPROVEMENT = "Efficiency Improvement";
    private static final String API_CATEGORY_PREVENTION = "Prevention";
    private static final String API_CATEGORY_COMPLIANCE = "Compliance";
    private static final String API_CATEGORY_UNKNOWN = "Unknown";

    private static final Logger logger = LogManager.getLogger();

    private final ActionExecutionDTO actionExecutionDTO;

    /**
     * Creates a new instance of {@link SdkActionInformationProvider} class.
     *
     * @param actionExecutionDTO the DTO that this class provides information about.
     */
    public SdkActionInformationProvider(@Nonnull ActionExecutionDTO actionExecutionDTO) {
        Objects.requireNonNull(actionExecutionDTO);
        this.actionExecutionDTO = actionExecutionDTO;
    }

    private ActionItemDTO getPrimaryAction() {
        if (this.actionExecutionDTO.getActionItemCount() > 0) {
            return this.actionExecutionDTO.getActionItem(0);
        } else {
            throw new IllegalStateException("The action \"" + actionExecutionDTO + "\" is missing"
                + " action items and cannot be converted.");
        }
    }

    @Override
    public long getRecommendationId() {
        return actionExecutionDTO.getActionOid();
    }

    @Override
    public long getUuid() {
        return Long.valueOf(getPrimaryAction().getUuid());
    }

    @Override
    public Optional<ActionMode> getActionMode() {
        return Optional.empty();
    }

    @Override
    public Optional<ActionState> getActionState() {
        return mapExternalStateToApiState(actionExecutionDTO.getActionState());
    }

    @Override
    public List<String> getPrerequisiteDescriptionList() {
        return Collections.emptyList();
    }

    @Override
    public String getRiskDescription() {
        return getPrimaryAction().getRisk().getDescription();
    }

    @Override
    public String getCategory() {
        return mapSdkRiskCategoryToApi(getPrimaryAction().getRisk().getCategory());
    }

    @Override
    public String getSeverity() {
        return getPrimaryAction().getRisk().getSeverity().name();
    }

    @Override
    public Set<String> getReasonCommodities() {
        if (getPrimaryAction().hasRisk()) {
            return getPrimaryAction().getRisk().getAffectedCommodityList()
                .stream().map(CommodityTypeMapping::getApiCommodityType)
                .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    @Override
    public ActionType getActionType() {
        return mapExternalStateToApiActionType(actionExecutionDTO.getActionType());
    }

    @Override
    public Optional<PolicyApiDTO> getRelatedPolicy() {
        return Optional.empty();
    }

    @Override
    public long getCreationTime() {
        return actionExecutionDTO.getCreateTime();
    }

    @Override
    public Optional<Long> getUpdatingTime() {
        return actionExecutionDTO.hasUpdateTime()
            ? Optional.of(actionExecutionDTO.getUpdateTime()) : Optional.empty();
    }

    @Override
    public Optional<String> getAcceptingUser() {
        return actionExecutionDTO.hasAcceptedBy()
            ? Optional.of(actionExecutionDTO.getAcceptedBy()) : Optional.empty();
    }

    @Override
    public ServiceEntityApiDTO getTarget() {
        final ActionItemDTO actionItemDTO = getPrimaryAction();
        final String clusterId = getContextValue(actionItemDTO,
            SDKConstants.CURRENT_HOST_CLUSTER_ID);
        final String clusterName = getContextValue(actionItemDTO,
            SDKConstants.CURRENT_HOST_CLUSTER_DISPLAY_NAME);
        return convertSdkEntityToApi(actionItemDTO.getTargetSE(), clusterId, clusterName);
    }

    @Override
    public Optional<ServiceEntityApiDTO> getCurrentEntity() {
        final ActionItemDTO actionItemDTO = getPrimaryAction();
        if (actionItemDTO.hasCurrentSE()) {
            final String clusterId = getContextValue(actionItemDTO, SDKConstants.CURRENT_HOST_CLUSTER_ID);
            final String clusterName = getContextValue(actionItemDTO,
                SDKConstants.CURRENT_HOST_CLUSTER_DISPLAY_NAME);
            return Optional.of(convertSdkEntityToApi(getPrimaryAction().getCurrentSE(), clusterId,
                clusterName));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ServiceEntityApiDTO> getNewEntity() {
        final ActionItemDTO actionItemDTO = getPrimaryAction();
        if (getPrimaryAction().hasNewSE()) {
            final String clusterId = getContextValue(actionItemDTO, SDKConstants.NEW_HOST_CLUSTER_ID);
            final String clusterName = getContextValue(actionItemDTO,
                SDKConstants.NEW_HOST_CLUSTER_DISPLAY_NAME);
            return Optional.of(convertSdkEntityToApi(getPrimaryAction().getNewSE(), clusterId,
                clusterName));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String getDescription() {
        return getPrimaryAction().getDescription();
    }

    @Override
    public List<StatApiDTO> getStats() {
        return Collections.emptyList();
    }

    @Override
    public Optional<String> getCurrentValue() {
        return getCurrentValue(getPrimaryAction());
    }

    private Optional<String> getCurrentValue(ActionItemDTO actionItemDTO) {
        switch (actionItemDTO.getActionType()) {
            case MOVE:
            case SCALE:
            case START:
            case RECONFIGURE:
            case PROVISION:
                return actionItemDTO.hasCurrentSE()
                    ? Optional.of(String.valueOf(actionItemDTO.getCurrentSE()
                    .getTurbonomicInternalId()))  : Optional.empty();
            case RESIZE:
                return actionItemDTO.hasCurrentComm()
                    ? Optional.of(String.format(FORMAT_FOR_ACTION_VALUES,
                    actionItemDTO.getCurrentComm().getCapacity())) : Optional.empty();
            default:
                return Optional.empty();
        }
    }

    @Override
    public Optional<String> getNewValue() {
        return getNewValue(getPrimaryAction());
    }

    private Optional<String> getNewValue(ActionItemDTO actionItemDTO) {
        switch (actionItemDTO.getActionType()) {
            case MOVE:
            case SCALE:
            case START:
            case RECONFIGURE:
            case PROVISION:
                return actionItemDTO.hasNewSE()
                    ? Optional.of(String.valueOf(actionItemDTO.getNewSE()
                    .getTurbonomicInternalId()))
                    : Optional.empty();
            case RESIZE:
                return actionItemDTO.hasNewComm()
                    ? Optional.of(String.format(FORMAT_FOR_ACTION_VALUES,
                    actionItemDTO.getNewComm().getCapacity())) : Optional.empty();
            default:
                return Optional.empty();
        }
    }

    @Nonnull
    @Override
    public Optional<String> getValueUnit() {
        final ActionItemDTO.Risk risk = getPrimaryAction().getRisk();
        if (getPrimaryAction().hasRisk()  && risk.getAffectedCommodityCount() > 0) {
            return CommodityTypeMapping.getCommodityUnitsForActions(risk.getAffectedCommodity(0).getNumber(),
                getPrimaryAction().getTargetSE().getEntityType().getNumber());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ActionExecutionCharacteristicApiDTO> getActionExecutionCharacteristics() {
        if (getPrimaryAction().hasCharacteristics()) {
            final ActionExecutionCharacteristicApiDTO actionExecutionCharacteristicApiDTO
                = new ActionExecutionCharacteristicApiDTO();
            final ActionItemDTO.ExecutionCharacteristics characteristics =
                getPrimaryAction().getCharacteristics();
            if (characteristics.hasDisruptive()) {
                actionExecutionCharacteristicApiDTO.setDisruptiveness(
                    characteristics.getDisruptive() ? ActionDisruptiveness.DISRUPTIVE
                        : ActionDisruptiveness.NON_DISRUPTIVE);
            }
            if (characteristics.hasReversible()) {
                actionExecutionCharacteristicApiDTO.setReversibility(
                    characteristics.getReversible() ? ActionReversibility.REVERSIBLE
                        : ActionReversibility.IRREVERSIBLE);
            }
            return Optional.of(actionExecutionCharacteristicApiDTO);
        } else {
            return Optional.empty();
        }
    }

    @Override
    @Nonnull
    public List<ActionApiDTO> getCompoundActions() {
        if (actionExecutionDTO.getActionItemCount() > 1
              || ActionItemDTO.ActionType.MOVE == getPrimaryAction().getActionType()
              || ActionItemDTO.ActionType.SCALE == getPrimaryAction().getActionType()) {
            List<ActionApiDTO> actions = Lists.newArrayList();
            for (ActionItemDTO item : actionExecutionDTO.getActionItemList()) {
                ActionApiDTO actionApiDTO = new ActionApiDTO();
                actionApiDTO.setTarget(new ServiceEntityApiDTO());
                actionApiDTO.setCurrentEntity(new ServiceEntityApiDTO());
                actionApiDTO.setNewEntity(new ServiceEntityApiDTO());
                actionApiDTO.setDetails(item.getDescription());

                actionApiDTO.setActionType(mapExternalStateToApiActionType(item.getActionType()));
                // Set entity DTO fields for target, source (if needed) and destination entities
                actionApiDTO.setTarget(getTarget());

                getNewValue(item).ifPresent(actionApiDTO::setNewValue);
                actionApiDTO.setNewEntity(convertSdkEntityToApi(item.getNewSE(), null, null));

                final boolean hasSource = item.hasCurrentSE();
                if (hasSource) {
                    getCurrentValue(item).ifPresent(actionApiDTO::setCurrentValue);
                    actionApiDTO.setCurrentEntity(convertSdkEntityToApi(item.getCurrentSE(), null, null));
                }
                actions.add(actionApiDTO);
            }
            return actions;
        } else {
            return Collections.emptyList();
        }
    }

    @Nullable
    private static String getContextValue(@Nonnull ActionItemDTO actionItemDTO,
                                          @Nonnull String key) {
        return actionItemDTO.getContextDataList()
            .stream()
            .filter(c -> key.equals(c.getContextKey()))
            .map(CommonDTO.ContextData::getContextValue)
            .findAny().orElse(null);
    }

    private static ServiceEntityApiDTO convertSdkEntityToApi(@Nonnull CommonDTO.EntityDTO entityDTO,
                                                             @Nullable String clusterId,
                                                             @Nullable String clusterName) {
        final ServiceEntityApiDTO converted = new ServiceEntityApiDTO();
        converted.setUuid(String.valueOf(entityDTO.getTurbonomicInternalId()));
        converted.setDisplayName(entityDTO.getDisplayName());
        converted.setClassName(EntityTypeMapping.getApiEntityType(entityDTO.getEntityType()).name());
        converted.setState(mapSdkStateToApiState(entityDTO).name());
        converted.setDiscoveredBy(getEntityTargetApiDto(entityDTO));
        converted.setVendorIds(getVendorIds(entityDTO));
        final Map<String, List<String>> tagsMap = getTags(entityDTO);
        if (!tagsMap.isEmpty()) {
            converted.setTags(tagsMap);
        }

        if (clusterId != null || clusterName != null) {
            BaseApiDTO baseApiDTO = new BaseApiDTO();
            if (clusterId != null) {
                baseApiDTO.setUuid(clusterId);
            }
            if (clusterName != null) {
                baseApiDTO.setDisplayName(clusterName);
            }
            baseApiDTO.setClassName("Cluster");
            converted.setConnectedEntities(Collections.singletonList(baseApiDTO));
        }
        return converted;
    }

    private static Map<String, List<String>> getTags(CommonDTO.EntityDTO entityDTO) {
        Map<String, List<String>> tags = new HashMap<>();
        for (CommonDTO.EntityDTO.EntityProperty property : entityDTO.getEntityPropertiesList()) {
            if (SDKUtil.VC_TAGS_NAMESPACE.equals(property.getNamespace())) {
                tags.computeIfAbsent(property.getName(), e -> new ArrayList<>())
                    .add(property.getValue());
            }
        }

        return tags;
    }

    private static Map<String, String> getVendorIds(CommonDTO.EntityDTO entityDTO) {
        Optional<String> localName = getEntityNamespaceAndProperty(entityDTO,
              SupplyChainConstants.LOCAL_NAME)
            .map(Pair::getValue);
        Optional<String> targetAddress = getEntityNamespaceAndProperty(entityDTO, TARGET_ADDRESS)
            .map(Pair::getValue);
        return Collections.singletonMap(targetAddress.orElse(""),
            localName.orElse(""));

    }

    private static TargetApiDTO getEntityTargetApiDto(CommonDTO.EntityDTO entityDTO) {
        TargetApiDTO targetApiDTO = new TargetApiDTO();
        getEntityNamespaceAndProperty(entityDTO, TARGET_ADDRESS)
            .map(Pair::getValue)
            .ifPresent(targetApiDTO::setDisplayName);
        getEntityNamespaceAndProperty(entityDTO, SupplyChainConstants.TARGET_TYPE)
            .map(Pair::getValue)
            .ifPresent(targetApiDTO::setType);

        return targetApiDTO;
    }

    private static Optional<Pair<String, String>> getEntityNamespaceAndProperty(
                                               CommonDTO.EntityDTO entityDTO,
                                               String name) {
        return entityDTO.getEntityPropertiesList()
            .stream()
            .filter(e -> name.equals(e.getName()))
            .filter(e -> e.getValue() != null)
            .filter(e -> e.getNamespace() != null)
            .map(v -> Pair.of(v.getNamespace(), v.getValue()))
            .findAny();
    }

    private static Optional<ActionState> mapExternalStateToApiState(
        ActionExecution.ActionResponseState actionState) {
        switch (actionState) {
            case PENDING_ACCEPT:
                return  Optional.of(ActionState.READY);
            case ACCEPTED:
                return Optional.of(ActionState.ACCEPTED);
            case REJECTED:
                return Optional.of(ActionState.REJECTED);
            case QUEUED:
                return Optional.of(ActionState.QUEUED);
            case SUCCEEDED:
                return Optional.of(ActionState.SUCCEEDED);
            case IN_PROGRESS:
                return Optional.of(ActionState.IN_PROGRESS);
            case FAILING:
                return Optional.of(ActionState.FAILING);
            case FAILED:
                return Optional.of(ActionState.FAILED);
            case CLEARED:
                return Optional.of(ActionState.CLEARED);
            default:
                logger.warn("The action state \"{}\" cannot be not mapped to an API state.",
                    actionState);
                return Optional.empty();
        }
    }

    private static ActionType mapExternalStateToApiActionType(
        ActionItemDTO.ActionType actionType) {
        switch (actionType) {
            case MOVE:
                return ActionType.MOVE;
            case SCALE:
                return ActionType.SCALE;
            case NONE:
                return ActionType.NONE;
            case RESIZE:
                return ActionType.RESIZE;
            case START:
                return ActionType.START;
            case PROVISION:
                return ActionType.PROVISION;
            case SUSPEND:
                return ActionType.SUSPEND;
            case RECONFIGURE:
                return ActionType.RECONFIGURE;
            case DELETE:
                return ActionType.DELETE;
            default:
                throw new IllegalStateException("The SDK action type \"" + actionType + "\" does "
                  + "not have a corresponding API type.");
        }
    }

    private static EntityState mapSdkStateToApiState(CommonDTO.EntityDTO entityDTO) {
        EntityState entityState = EntityState.UNKNOWN;

        // retrieve entity state from dto
        CommonDTO.EntityDTO.PowerState powerState = entityDTO.getPowerState();
        switch (powerState) {
            case POWERED_OFF:
                entityState = EntityState.IDLE;
                break;
            case POWERED_ON:
                entityState = EntityState.ACTIVE;
                break;
            case SUSPENDED:
                entityState = EntityState.SUSPEND;
                break;
        }

        // Handle some power states that are specific for PMs
        if (entityDTO.getEntityType() == CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE) {

            // Some hypervisors (like VC) can have a PM in maintenance and failover at the same time.
            // In this case, given that we want to show only a single state of the entity, we choose
            // to show maintenance, as the stronger of the 2 states.
            // So a server in maintenance will override a failover state.
            if (entityDTO.getMaintenance()) {
                entityState = EntityState.MAINTENANCE;
            } else if (entityDTO.getPhysicalMachineData().getPmState().getFailover()) {
                entityState = EntityState.FAILOVER;
            }
        }

        return entityState;
    }


    /**
     * Map an SDK category to an equivalent API category string.
     *
     * @param category The {@link ActionItemDTO.Risk.Category}.
     * @return A string representing the action category.
     */
    @Nonnull
    public static String mapSdkRiskCategoryToApi(
          @Nonnull final ActionItemDTO.Risk.Category category) {
        switch (category) {
            case PERFORMANCE_ASSURANCE:
                return API_CATEGORY_PERFORMANCE_ASSURANCE;
            case EFFICIENCY_IMPROVEMENT:
                return API_CATEGORY_EFFICIENCY_IMPROVEMENT;
            case PREVENTION:
                return API_CATEGORY_PREVENTION;
            case COMPLIANCE:
                return API_CATEGORY_COMPLIANCE;
            default:
                return API_CATEGORY_UNKNOWN;
        }
    }

}
