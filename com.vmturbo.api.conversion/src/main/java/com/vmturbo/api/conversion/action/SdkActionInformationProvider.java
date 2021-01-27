package com.vmturbo.api.conversion.action;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.conversion.entity.EntityTypeMapping;
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
import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 * This provides information about action for conversion based on an {@code ActionExecutionDTO}
 * object.
 */
public class SdkActionInformationProvider implements ActionInformationProvider {
    private static final String TARGET_ADDRESS = "targetAddress";
    private static final String FORMAT_FOR_ACTION_VALUES = "%.1f";

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

    private ActionExecution.ActionItemDTO getPrimaryAction() {
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
        return getPrimaryAction().getRisk().getCategory().name();
    }

    @Override
    public String getSeverity() {
        return getPrimaryAction().getRisk().getSeverity().name();
    }

    @Override
    public Set<String> getReasonCommodities() {
        if (getPrimaryAction().hasRisk()) {
            return getPrimaryAction().getRisk().getAffectedCommodityList()
                .stream().map(CommonDTO.CommodityDTO.CommodityType::toString)
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
        return convertSdkEntityToApi(getPrimaryAction().getTargetSE());
    }

    @Override
    public Optional<ServiceEntityApiDTO> getCurrentEntity() {
        if (getPrimaryAction().hasCurrentSE()) {
            return Optional.of(convertSdkEntityToApi(getPrimaryAction().getCurrentSE()));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ServiceEntityApiDTO> getNewEntity() {
        if (getPrimaryAction().hasNewSE()) {
            return Optional.of(convertSdkEntityToApi(getPrimaryAction().getNewSE()));
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
        switch (actionExecutionDTO.getActionType()) {
            case MOVE:
            case SCALE:
            case START:
            case RECONFIGURE:
            case PROVISION:
                return getPrimaryAction().hasCurrentSE()
                    ? Optional.of(String.valueOf(getPrimaryAction().getCurrentSE()
                    .getTurbonomicInternalId()))  : Optional.empty();
            case RESIZE:
                return getPrimaryAction().hasCurrentComm()
                    ? Optional.of(String.format(FORMAT_FOR_ACTION_VALUES,
                        getPrimaryAction().getCurrentComm().getCapacity())) : Optional.empty();
            default:
                return Optional.empty();
        }
    }

    @Override
    public Optional<String> getNewValue() {
        switch (actionExecutionDTO.getActionType()) {
            case MOVE:
            case SCALE:
            case START:
            case RECONFIGURE:
            case PROVISION:
                return getPrimaryAction().hasNewSE()
                    ? Optional.of(String.valueOf(getPrimaryAction().getNewSE()
                      .getTurbonomicInternalId()))
                    : Optional.empty();
            case RESIZE:
                return getPrimaryAction().hasNewComm()
                    ? Optional.of(String.format(FORMAT_FOR_ACTION_VALUES,
                        getPrimaryAction().getNewComm().getCapacity())) : Optional.empty();
            default:
                return Optional.empty();
        }
    }

    @Override
    public Optional<ActionExecutionCharacteristicApiDTO> getActionExecutionCharacteristics() {
        if (getPrimaryAction().hasCharacteristics()) {
            final ActionExecutionCharacteristicApiDTO actionExecutionCharacteristicApiDTO
                = new ActionExecutionCharacteristicApiDTO();
            final ActionExecution.ActionItemDTO.ExecutionCharacteristics characteristics =
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

    private static ServiceEntityApiDTO convertSdkEntityToApi(CommonDTO.EntityDTO entityDTO) {
        final ServiceEntityApiDTO converted = new ServiceEntityApiDTO();
        converted.setUuid(String.valueOf(entityDTO.getTurbonomicInternalId()));
        converted.setDisplayName(entityDTO.getDisplayName());
        converted.setClassName(EntityTypeMapping.getApiEntityType(entityDTO.getEntityType()).name());
        converted.setState(mapSdkStateToApiState(entityDTO).name());
        converted.setDiscoveredBy(getEntityTargetApiDto(entityDTO));
        converted.setVendorIds(getVendorIds(entityDTO));
        return converted;
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
        ActionExecution.ActionItemDTO.ActionType actionType) {
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
}
