package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyAtomicActionsCommodityType;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyCommodityType;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyCommodityTypes;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyEntityTypeAndName;
import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.ENTITY_WITH_ADDITIONAL_COMMODITY_CHANGES;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.jsonwebtoken.lang.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityNewCapacityEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionVirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.HCIUtils;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.platform.sdk.common.util.Pair;

public class ActionDescriptionBuilder {

    private static final Logger logger = LogManager.getLogger();

    // Static strings for use in message contents.
    private static final String MOVE = "Move";
    private static final String SCALE = "Scale";
    private static final String UP = "up";
    private static final String DOWN = "down";
    private static final String OF = " of ";
    private static final String RESERVATION = " and Reservation";
    // pass in methodName, name of entity, and entity OID
    private static final String ENTITY_NOT_FOUND_WARN_MSG = "{} {} Entity {} doesn't exist in the entities snapshot";
    private static final Map<CommodityType, String> CLOUD_SCALE_ACTION_COMMODITY_TYPE_DISPLAYNAME
            = ImmutableMap.of(CommodityType.STORAGE_ACCESS, "IOPS", CommodityType.STORAGE_AMOUNT, "Disk size");

    // Cloud native entities with CPU values in millicores.
    private static final Set<Integer> CLOUD_NATIVE_ENTITIES = ImmutableSet.of(EntityType.CONTAINER_VALUE,
        EntityType.CONTAINER_SPEC_VALUE, EntityType.WORKLOAD_CONTROLLER_VALUE, EntityType.NAMESPACE_VALUE);
    private static final String COMMA_SPACE = ", ";
    private static final String AZURE = "Azure";


    // Static patterns used for dynamically-constructed message contents.
    //
    // We're going to model these pattern strings as an enum, containing a set of
    // ThreadLocal-based MessageFormat instances based on each pattern. Previously, we would pass
    // the pattern strings to MessageFormat.format(pattern, args...), but this approach creates a
    // new MessageFormat instance and does a lookup to find the default locale on every invocation.
    // In an environment with hundreds of thousands of actions, this kind of things adds up.
    //
    // The patterns strings are static, so we might as well reuse the MessageFormat instances for
    // each, instead of having MessageFormat create new ones every time.
    //
    // Note that MessageFormat instances are not thread-safe, so we're using ThreadLocals, which
    // gives each thread access to it's own MessageFormat instances.
    //
    private enum ActionMessageFormat {
        ACTION_DESCRIPTION_PROVISION_BY_SUPPLY("Provision {0}{1}"),
        ACTION_DESCRIPTION_PROVISION_BY_DEMAND("Provision {0} similar to {1} with scaled up {2} due to {3}"),
        ACTION_DESCRIPTION_LICENSED_PROVISION_BY_SUPPLY("Provision Licensed {0}{1}"),
        ACTION_DESCRIPTION_LICENSED_PROVISION_BY_DEMAND("Provision Licensed {0} similar to {1} with scaled up {2} due to {3}"),
        ACTION_DESCRIPTION_ACTIVATE("Start {0}"),
        ACTION_DESCRIPTION_DEACTIVATE("{0} {1}"),
        ACTION_DESCRIPTION_LICENSED_DEACTIVATE("{0} Licensed {1}"),
        ACTION_DESCRIPTION_DELETE("Delete wasted file ''{0}'' from {1} to free up {2}"),
        ACTION_DESCRIPTION_DELETE_CLOUD_NO_ACCOUNT("Delete Unattached {0} Volume {1}"),
        ACTION_DESCRIPTION_DELETE_CLOUD("Delete Unattached {0} Volume {1} from {2}"),
        ACTION_DESCRIPTION_DELETE_APPLICATION_SERVICE("Delete Empty {0} App Service Plan {1} from {2}"),
        ACTION_DESCRIPTION_DELETE_APPLICATION_SERVICE_DEFAULT("Delete Empty App Service Plan {0}"),
        ACTION_DESCRIPTION_ATOMIC_RESIZE("Resize {0} for {1}"),
        ACTION_DESCRIPTION_RESIZE_REMOVE_LIMIT("Remove {0} limit on entity {1}"),
        ACTION_DESCRIPTION_RESIZE("Resize {0} {1} for {2} from {3} to {4}"),
        ACTION_DESCRIPTION_RESIZE_RESERVATION("Resize {0} {1} reservation for {2} from {3} to {4}"),
        ACTION_DESCRIPTION_RECONFIGURE_REASON_COMMODITIES("Reconfigure {0} to {1} {2}"),
        ACTION_DESCRIPTION_RECONFIGURE_ADD_REASON_COMMODITIES("Reconfigure {0} to provide {1} from {2}"),
        ACTION_DESCRIPTION_RECONFIGURE_REASON_SETTINGS("Reconfigure {0}"),
        ACTION_DESCRIPTION_RECONFIGURE_TAINT_COMMODITIES("Reconfigure {0}"),
        ACTION_DESCRIPTION_RECONFIGURE_WITHOUT_SOURCE("Reconfigure {0} as it is unplaced"),
        ACTION_DESCRIPTION_RECONFIGURE_INCLUDE_POLICY("Reconfigure {0} to comply with policy {1}"),
        ACTION_DESCRIPTION_MOVE_WITHOUT_SOURCE("Start {0} on {1}"),
        ACTION_DESCRIPTION_MOVE("{0} {1}{2} from {3} to {4}"),
        ACTION_DESCRIPTION_SCALE_COMMODITY_CHANGE("Scale {0} {1} for {2} from {3} to {4}"),
        ACTION_DESCRIPTION_SCALE_COMMODITY_CHANGE_WITH_CURRENT_PROVIDER("Scale {0} {1} for {2} on {3} from {4} to {5}"),
        ACTION_DESCRIPTION_SCALE_ADDITIONAL_COMMODITY_CHANGE("{0} {1} from {2} to {3}"),
        ACTION_DESCRIPTION_BUYRI("Buy {0} {1} RIs for {2} in {3}"),
        ACTION_DESCRIPTION_ALLOCATE("Increase RI coverage for {0} ({1}) in {2}"),
        CONTAINER_VCPU_MILLICORES(String.format("{0,number,integer} %s", CommodityTypeMapping.CPU_MILLICORE)),
        VCPU_CORES(String.format("{0,number,integer} %s", CommodityTypeMapping.CPU_CORE)),
        PROCESSING_UNIT(String.format("{0} %s", CommodityTypeMapping.PROCESSING_UNIT)),
        STORAGE_ACCESS_IOPS("{0,number,integer} IOPS"),
        IO_THROUGHPUT_MBPS("{0,number,integer} MB/s"),
        SIMPLE("{0, number, integer}"),
        ACTION_DESCRIPTION_ACCOUNT_NAME(" in {0}"),
        ACTION_DESCRIPTION_RECONFIGURE_LABEL_COMMODITIES("Reconfigure nodeSelector for {0}");


        private final ThreadLocal<MessageFormat> messageFormat;

        ActionMessageFormat(final String format) {
            messageFormat = ThreadLocal.withInitial(() -> new MessageFormat(format));
        }

        public String format(Object... args) {
            return messageFormat.get().format(args);
        }
    }

    // Commodities in actions mapped to their default units.
    // For example, vMem commodity has its default capacity unit as KB.
    private static final ImmutableMap<CommodityDTO.CommodityType, Long> commodityTypeToDefaultUnits =
        new ImmutableMap.Builder<CommodityDTO.CommodityType, Long>()
            .put(CommodityDTO.CommodityType.VMEM, Units.KBYTE)
            .put(CommodityDTO.CommodityType.VMEM_REQUEST, Units.KBYTE)
            .put(CommodityDTO.CommodityType.VMEM_LIMIT_QUOTA, Units.KBYTE)
            .put(CommodityDTO.CommodityType.STORAGE_AMOUNT, Units.MBYTE)
            .put(CommodityDTO.CommodityType.STORAGE_PROVISIONED, Units.MBYTE)
            .put(CommodityDTO.CommodityType.HEAP, Units.KBYTE)
            .put(CommodityDTO.CommodityType.MEM, Units.KBYTE)
            .put(CommodityDTO.CommodityType.DB_MEM, Units.KBYTE)
            .put(CommodityDTO.CommodityType.VSTORAGE, Units.MBYTE)
            .build();

    private static final Map<CommodityDTO.CommodityType, ActionMessageFormat>
            COMMODITY_TYPE_ACTION_MESSAGE_FORMAT_MAP =
            new ImmutableMap.Builder<CommodityDTO.CommodityType, ActionMessageFormat>()
                    .put(CommodityType.STORAGE_ACCESS, ActionMessageFormat.STORAGE_ACCESS_IOPS)
                    .put(CommodityType.IO_THROUGHPUT, ActionMessageFormat.IO_THROUGHPUT_MBPS)
                    .put(CommodityType.PROCESSING_UNITS, ActionMessageFormat.PROCESSING_UNIT)
                    .build();

    /**
     * Builds the action description to be passed to the API component. Forming the description was
     * done initially in the API component, but it was moved to AO to take advantage of
     * pagination.
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @param recommendation {@link ActionDTO.Action} the action object.
     * @return The final action description string that will be displayed in the UI.
     */
    public static String buildActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                @Nonnull final ActionDTO.Action recommendation)
            throws UnsupportedActionException {
        final ActionInfo info = recommendation.getInfo();
        switch(info.getActionTypeCase()) {
            case MOVE:
            case SCALE:
                return getMoveOrScaleActionDescription(entitiesSnapshot, recommendation);
            case ALLOCATE:
                return getAllocateActionDescription(entitiesSnapshot, recommendation);
            case RECONFIGURE:
                return getReconfigureActionDescription(entitiesSnapshot, recommendation);
            case PROVISION:
                return getProvisionActionDescription(entitiesSnapshot, recommendation);
            case ATOMICRESIZE:
                return getAtomicResizeActionDescription(entitiesSnapshot, recommendation);
            case RESIZE:
                return getResizeActionDescription(entitiesSnapshot, recommendation);
            case ACTIVATE:
                return getActivateActionDescription(entitiesSnapshot, recommendation);
            case DEACTIVATE:
                return getDeactivateActionDescription(entitiesSnapshot, recommendation);
            case DELETE:
                 return getDeleteActionDescription(entitiesSnapshot, recommendation);
            case BUYRI:
                return getRIBuyActionDescription(entitiesSnapshot, recommendation);
            default:
                throw new UnsupportedActionException(recommendation);
        }
    }

    /**
     * Builds the description for a Atomic Resize action.
     *
     * <p>e.g. description for merged actions on workload controller:
     * "Resize VCPU Request,VMem Limit,VMem Request,VCPU Limit
     *  for Workload Controller controller1_test"
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     *                                                      information.
     * @param recommendation the Action DTO
     * @return The Atomic Resize action description.
     */
    private static String getAtomicResizeActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                           @Nonnull final ActionDTO.Action recommendation) {
        AtomicResize atomicResize = recommendation.getInfo().getAtomicResize();

        // Target entity for the resize
        long entityId = atomicResize.getExecutionTarget().getId();
        Optional<ActionPartialEntity> optEntity = entitiesSnapshot.getEntityFromOid(entityId);
        if (!optEntity.isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getAtomicResizeActionDescription ", "entityId", entityId);
            return "";
        }

        ActionPartialEntity targetEntity = optEntity.get();

        Set<TopologyDTO.CommodityType> commodityTypes
                = atomicResize.getResizesList().stream()
                    .map(resize -> resize.getCommodityType())
                    .collect(Collectors.toSet());

        List<String> formattedCommodityTypes
                = commodityTypes.stream()
                    .map(commType -> beautifyAtomicActionsCommodityType(commType))
                    .collect(Collectors.toList());

        ActionMessageFormat messageFormat = ActionMessageFormat.ACTION_DESCRIPTION_ATOMIC_RESIZE;
        return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                messageFormat.format(String.join(",", formattedCommodityTypes),
                        beautifyEntityTypeAndName(targetEntity)));
    }

    /**
     * Builds the Resize action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @return The Resize action description.
     */
    private static String getResizeActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                              @Nonnull final ActionDTO.Action recommendation) {
        Resize resize = recommendation.getInfo().getResize();
        long entityId = resize.getTarget().getId();
        Optional<ActionPartialEntity> optEntity = entitiesSnapshot.getEntityFromOid(entityId);
        if (!optEntity.isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getResizeActionDescription ", "entityId", entityId);
            return "";
        }

        ActionPartialEntity entity = optEntity.get();
        final CommodityDTO.CommodityType commodityType = CommodityDTO.CommodityType
                .forNumber(resize.getCommodityType().getType());
        // Construct the description based on the attribute type
        if (resize.getCommodityAttribute() == CommodityAttribute.LIMIT) {
            return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                    ActionMessageFormat.ACTION_DESCRIPTION_RESIZE_REMOVE_LIMIT.format(
                            beautifyCommodityType(resize.getCommodityType()),
                            beautifyEntityTypeAndName(entity)));
        }

        String commodity = beautifyCommodityType(resize.getCommodityType());
        ActionVirtualMachineInfo vmInfo = entity.getTypeSpecificInfo().getVirtualMachine();

        if (commodityType == CommodityType.VCPU && vmInfo.getCpuCoreMhz() != 0.0) {
            Double cpuReservation = vmInfo.getReservationInfoMap().get(CommodityType.CPU_VALUE);
            if (cpuReservation != null
                    && resize.getNewCapacity() * vmInfo.getCpuCoreMhz() < cpuReservation) {
                commodity += RESERVATION;
            }
        }
        ActionMessageFormat messageFormat =
                        resize.getCommodityAttribute() == CommodityAttribute.CAPACITY
                                        ? ActionMessageFormat.ACTION_DESCRIPTION_RESIZE
                                        : ActionMessageFormat.ACTION_DESCRIPTION_RESIZE_RESERVATION;
        if (commodityType == CommodityType.VMEM) {
            Double memReservation = vmInfo.getReservationInfoMap().get(CommodityType.MEM_VALUE);
            if (memReservation != null && resize.getNewCapacity() < memReservation) {
                commodity += RESERVATION;
            }
        }

        if (commodityType == CommodityDTO.CommodityType.VSTORAGE) {
            commodity += getPartition(entity, resize);
        }

        return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                messageFormat.format(resize.getNewCapacity() > resize.getOldCapacity() ? UP : DOWN,
                        commodity, beautifyEntityTypeAndName(entity),
                        formatResizeActionCommodityValue(commodityType, entity.getEntityType(),
                                resize.getOldCapacity(), false),
                        formatResizeActionCommodityValue(commodityType, entity.getEntityType(),
                                resize.getNewCapacity(), true)));
    }

    @Nonnull
    private static String getPartition(@Nonnull ActionPartialEntity entity,
            @Nonnull Resize resize) {
        ActionEntityTypeSpecificInfo info = entity.getTypeSpecificInfo();
        if (info == null) {
            logger.warn("VM {} is missing the type specific info", entity.getDisplayName());
            return "";
        }

        ActionVirtualMachineInfo vmInfo = info.getVirtualMachine();
        if (vmInfo == null) {
            logger.warn("VM {} is missing the VM info", entity.getDisplayName());
            return "";
        }

        Map<String, String> partitions = vmInfo.getPartitionsMap();
        if (Collections.isEmpty(partitions)) {
            logger.warn("VM {} is missing the partition info", entity.getDisplayName());
            return "";
        }

        String key = resize.getCommodityType().getKey();
        if (Strings.isNullOrEmpty(key)) {
            logger.warn("VM {}: Resize vStorage action is missing the key",
                    entity.getDisplayName());
            return "";
        }

        String partition = partitions.get(key);
        if (Strings.isNullOrEmpty(partition)) {
            logger.warn("VM {}: Failed to find the partition for the key '{}'",
                    entity.getDisplayName(), key);
            return "";
        }

        return " " + partition;
    }

    /**
     * Builds the Reconfigure action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @return The Reconfigure action description.
     */
    private static String getReconfigureActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                   @Nonnull final ActionDTO.Action recommendation) {
        final String messageSeparator = COMMA_SPACE;
        final Reconfigure reconfigure = recommendation.getInfo().getReconfigure();
        final long entityId = reconfigure.getTarget().getId();
        Optional<ActionPartialEntity> targetEntityDTO = entitiesSnapshot.getEntityFromOid(entityId);
        if (!targetEntityDTO.isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getReconfigureActionDescription", "entityId", entityId);
            return "";
        }
        final ReconfigureExplanation explanation = recommendation.getExplanation().getReconfigure();
        if (reconfigure.hasSource() || reconfigure.getIsProvider()) {
            long sourceId = reconfigure.getSource().getId();
            Optional<ActionPartialEntity> currentEntityDTO = entitiesSnapshot.getEntityFromOid(
                sourceId);
            if (!currentEntityDTO.isPresent() && !reconfigure.getIsProvider()) {
                logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getReconfigureActionDescription", "sourceId", sourceId);
                return "";
            }
            if (!explanation.getReasonSettingsList().isEmpty()) {
                return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                        ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_REASON_SETTINGS.format(
                                beautifyEntityTypeAndName(targetEntityDTO.get())));
            } else {
                final List<TopologyDTO.CommodityType> reconfigureCommodities = explanation
                        .getReconfigureCommodityList()
                        .stream()
                        .map(ReasonCommodity::getCommodityType)
                        .collect(Collectors.toList());
                if (currentEntityDTO.isPresent() && reconfigure.getIsProvider()) {
                    return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                            ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_ADD_REASON_COMMODITIES.format(
                                    beautifyEntityTypeAndName(targetEntityDTO.get()),
                                    beautifyCommodityTypes(reconfigureCommodities),
                                    beautifyEntityTypeAndName(currentEntityDTO.get())));
                }
                final Set<String> taintReasons = ExplanationComposer
                        .getCommodityReasons(explanation.getReconfigureCommodityList(), CommodityType.TAINT_VALUE);
                if (!taintReasons.isEmpty()) {
                    return ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_TAINT_COMMODITIES
                            .format(beautifyEntityTypeAndName(targetEntityDTO.get()));
                }
                final Set<String> labelReasons = ExplanationComposer.getCommodityReasons(
                    explanation.getReconfigureCommodityList(), CommodityType.LABEL_VALUE);
                if (!labelReasons.isEmpty()) {
                    return ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_LABEL_COMMODITIES.format(
                        beautifyEntityTypeAndName(targetEntityDTO.get()));
                }
                return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                        ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_REASON_COMMODITIES.format(
                                beautifyEntityTypeAndName(targetEntityDTO.get()),
                                ((reconfigure.getIsProvider() && !reconfigure.getIsAddition())
                                        ? "remove" : "provide"),
                                beautifyCommodityTypes(reconfigureCommodities)));
            }
        } else if (reconfigure.getSettingChangeCount() > 0) {
            String policyName = explanation.getReasonSettingsList().stream()
                    .map(k -> entitiesSnapshot.getPolicyIdToDisplayName().get((long)k))
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(messageSeparator));
            return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                    ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_INCLUDE_POLICY.format(
                            beautifyEntityTypeAndName(targetEntityDTO.get()), policyName));

        } else {
            return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                    ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_WITHOUT_SOURCE.format(
                            beautifyEntityTypeAndName(targetEntityDTO.get())));
        }
    }

    /**
     * Builds Allocate action description.
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @param recommendation Action recommendation.
     * @return Allocate action description.
     */
    private static String getAllocateActionDescription(
            @Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
            @Nonnull final ActionDTO.Action recommendation) {
        final Allocate allocate = recommendation.getInfo().getAllocate();
        final long workloadEntityId = allocate.getTarget().getId();

        // Get workload
        final Optional<ActionPartialEntity> workloadEntityOpt = entitiesSnapshot.getEntityFromOid(
                workloadEntityId);
        if (!workloadEntityOpt.isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getAllocateActionDescription",
                    "workloadEntityId", workloadEntityId);
            return "";
        }
        final ActionPartialEntity workloadEntity = workloadEntityOpt.get();

        final long workloadTierId = allocate.getWorkloadTier().getId();
        final Optional<ActionPartialEntity> workloadTierOpt = entitiesSnapshot.getEntityFromOid(workloadTierId);
        if(!workloadTierOpt.isPresent()){
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getAllocateActionDescription",
                "workloadTierId", workloadTierId);
            return "";
        }
        final ActionPartialEntity workloadTier = workloadTierOpt.get();
        final String workloadTierName = workloadTier.getDisplayName();

        // Get account
        final Optional<EntityWithConnections> accountOpt = entitiesSnapshot
                .getOwnerAccountOfEntity(workloadEntityId);
        final String accountName;
        if (accountOpt.isPresent()) {
            accountName = accountOpt.get().getDisplayName();
        } else {
            logger.warn("Cannot find Business Account for Allocate action. VM - {} (ID: {})",
                    workloadEntity.getDisplayName(), workloadEntityId);
            accountName = "";
        }

        return ActionMessageFormat.ACTION_DESCRIPTION_ALLOCATE.format(
                beautifyEntityTypeAndName(workloadEntity), workloadTierName, accountName);
    }

    /**
     * Builds Move or Scale action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @param recommendation Action recommendation.
     * @return The action description.
     */
    private static String getMoveOrScaleActionDescription(
                @Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                @Nonnull final ActionDTO.Action recommendation) throws UnsupportedActionException {
        final Optional<ChangeProvider> primaryChange = ActionDTOUtil.getPrimaryChangeProvider(recommendation);
        if (primaryChange.isPresent()) {
            return getMoveOrScaleActionWithProviderChangeDescription(entitiesSnapshot, recommendation, primaryChange.get());
        } else {
            return getScaleActionWithoutProviderChangeDescription(entitiesSnapshot, recommendation);
        }

    }

    private static String getMoveOrScaleActionWithProviderChangeDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                                            @Nonnull final ActionDTO.Action recommendation,
                                                                            @Nonnull final ChangeProvider primaryChange)
            throws UnsupportedActionException {
        final boolean initialPlacement = ActionDTOUtil.getChangeProviderExplanationList(
                recommendation.getExplanation()).stream()
                .anyMatch(ChangeProviderExplanation::hasInitialPlacement);

        final boolean hasSource = !initialPlacement && primaryChange.hasSource();
        final long destinationEntityId = primaryChange.getDestination().getId();
        final long targetEntityId = ActionDTOUtil.getPrimaryEntity(recommendation).getId();
        Optional<ActionPartialEntity> optTargetEntity = entitiesSnapshot.getEntityFromOid(targetEntityId);
        if (!optTargetEntity.isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getMoveActionDescription", "targetEntityId", targetEntityId);
            return "";
        }
        Optional<ActionPartialEntity> optDestinationEntity = entitiesSnapshot.getEntityFromOid(destinationEntityId);
        if (!optDestinationEntity.isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getMoveActionDescription", "destinationEntityId", destinationEntityId);
            return "";
        }
        // All Move/RightSize actions should have a target entity and a destination.
        ActionPartialEntity targetEntityDTO = optTargetEntity.get();

        ActionPartialEntity newEntityDTO = optDestinationEntity.get();
        if (!hasSource) {
            return getDescriptionWithAccountName(entitiesSnapshot, targetEntityId,
                    ActionMessageFormat.ACTION_DESCRIPTION_MOVE_WITHOUT_SOURCE.format(
                            beautifyEntityTypeAndName(targetEntityDTO),
                            beautifyEntityTypeAndName(newEntityDTO)));
        } else {
            long sourceEntityId = primaryChange.getSource().getId();
            final Optional<ActionPartialEntity> optSourceEntity =
                    entitiesSnapshot.getEntityFromOid(sourceEntityId);
            if (!optSourceEntity.isPresent()) {
                logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getMoveActionDescription", "sourceEntityId", sourceEntityId);
                return "";
            }
            final ActionPartialEntity currentEntityDTO = optSourceEntity.get();
            int sourceType = currentEntityDTO.getEntityType();
            int destinationType = newEntityDTO.getEntityType();
            String currentLocation = currentEntityDTO.getDisplayName();
            String newLocation = newEntityDTO.getDisplayName();

            // We show scale if move is within same region. In cloud-to-cloud migration, there
            // is a region change, so we keep the action as MOVE, and don't change it to a SCALE.
            String verb = (recommendation.getInfo().getActionTypeCase() == ActionTypeCase.SCALE
                    || TopologyDTOUtil.isPrimaryTierEntityType(destinationType)
                    && TopologyDTOUtil.isPrimaryTierEntityType(sourceType)) ? SCALE : MOVE;
            if (TopologyDTOUtil.isMigrationAction(recommendation)) {
                verb = MOVE;
                Pair<String, String> regions = getRegions(recommendation, entitiesSnapshot);
                String sourceRegion = regions.getFirst();
                String destinationRegion = regions.getSecond();
                if (sourceRegion != null) {
                    currentLocation = sourceRegion;
                }
                if (destinationRegion != null) {
                    newLocation = destinationRegion;
                }
            }
            String resource = "";
            if (primaryChange.getResourceCount() > 0) {
                List<String> resources = primaryChange.getResourceList().stream()
                                .map(ActionEntity::getId)
                                .filter(id -> id != targetEntityId)
                                .map(id -> entitiesSnapshot.getEntityFromOid(id))
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .map(ActionDTOUtil::beautifyEntityTypeAndName)
                                .collect(Collectors.toList());
                if (!resources.isEmpty()) {
                    resource = resources.stream().sorted().collect(Collectors.joining(COMMA_SPACE)) + OF;
                }
            }
            String actionMessage = ActionMessageFormat.ACTION_DESCRIPTION_MOVE.format(verb, resource,
                    beautifyEntityTypeAndName(targetEntityDTO),
                    currentLocation,
                    newLocation);
            final Optional<String> additionalActionDescription;
            if (ENTITY_WITH_ADDITIONAL_COMMODITY_CHANGES.contains(optTargetEntity.get().getEntityType())) {
                additionalActionDescription =
                        getResizeProvidersForAdditionalCommodity(recommendation, targetEntityDTO, newEntityDTO);
            } else {
                additionalActionDescription = Optional.empty();
            }
            // add additionalActionDescription to actionMessage if not empty.
            return getDescriptionWithAccountName(entitiesSnapshot, targetEntityId,
                    additionalActionDescription.map(actionMessage::concat).orElse(actionMessage));

        }
    }

    /**
     * Helper method to retrieve additional action description involving additional
     * commodities participating in an action. Only for entities in {@link TopologyDTOUtil#ENTITY_WITH_ADDITIONAL_COMMODITY_CHANGES}.
     *
     * @param recommendation  current action recommendation.
     * @param targetEntityDTO target source entity.
     * @param newEntityDTO    destination entity.
     * @return Additional string for concat to action description.
     */
    private static Optional<String> getResizeProvidersForAdditionalCommodity(
            @Nonnull final ActionDTO.Action recommendation,
            @Nonnull final ActionPartialEntity targetEntityDTO,
            @Nonnull final ActionPartialEntity newEntityDTO) {
        if (recommendation.getInfo().hasScale()
                && !recommendation.getInfo().getScale().getCommodityResizesList().isEmpty()) {
            Collection<ResizeInfo> resizeInfos = recommendation.getInfo().getScale().getCommodityResizesList();
            return Optional.of(getAdditionalResizeCommodityMessage(resizeInfos, targetEntityDTO, newEntityDTO));
        }
        return Optional.empty();
    }

    @Nonnull
    private static String getAdditionalResizeCommodityMessage(
            @Nonnull Collection<ResizeInfo> resizeInfoList,
            @Nonnull ActionPartialEntity targetEntityDTO,
            @Nonnull ActionPartialEntity newEntityDTO) {
        final StringBuilder commodityMessageBuilder = new StringBuilder();
        final String messageSeparator = COMMA_SPACE;
        resizeInfoList.forEach(resizeInfo -> {
            final CommodityType resizeInfoCommodityType = CommodityType
                    .forNumber(resizeInfo.getCommodityType().getType());
            final String commodityTypeDisplayName = CLOUD_SCALE_ACTION_COMMODITY_TYPE_DISPLAYNAME
                    .getOrDefault(resizeInfoCommodityType, beautifyCommodityType(resizeInfo.getCommodityType()));
            final String oldVal = formatResizeActionCommodityValue(resizeInfoCommodityType,
                    targetEntityDTO.getEntityType(), resizeInfo.getOldCapacity(), false);
            final String newVal = formatResizeActionCommodityValue(resizeInfoCommodityType,
                    newEntityDTO.getEntityType(), resizeInfo.getNewCapacity(), true);
            final String verb = resizeInfo.getNewCapacity() > resizeInfo.getOldCapacity() ? UP : DOWN;
            commodityMessageBuilder.append(messageSeparator).append(ActionMessageFormat.ACTION_DESCRIPTION_SCALE_ADDITIONAL_COMMODITY_CHANGE.format(
                    commodityTypeDisplayName,
                    verb,
                    oldVal,
                    newVal
            ));
        });
        return commodityMessageBuilder.toString();
    }

    private static String getScaleActionWithoutProviderChangeDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                                         @Nonnull final ActionDTO.Action recommendation)
            throws UnsupportedActionException {
        final long targetEntityId = ActionDTOUtil.getPrimaryEntity(recommendation).getId();
        ActionPartialEntity optTargetEntity = entitiesSnapshot.getEntityFromOid(targetEntityId).orElse(null);
        if (optTargetEntity == null) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getScaleActionDescription", "targetEntityId", targetEntityId);
            return "";
        }
        Queue<ResizeInfo> resizeInfos = new LinkedList<>(recommendation.getInfo().getScale().getCommodityResizesList());
        if (CollectionUtils.isEmpty(resizeInfos)) {
            logger.warn("No commodity change for Scale action without provider change for {}", targetEntityId);
            return "";
        }
        ResizeInfo firstResizeInfo = resizeInfos.poll();
        final CommodityDTO.CommodityType firstResizeInfoCommodityType = CommodityDTO.CommodityType
                .forNumber(firstResizeInfo.getCommodityType().getType());
        String commodityTypeDisplayName = CLOUD_SCALE_ACTION_COMMODITY_TYPE_DISPLAYNAME
                .getOrDefault(firstResizeInfoCommodityType, beautifyCommodityType(firstResizeInfo.getCommodityType()));

        StringBuilder description;
        final ActionPartialEntity primaryProviderEntity = optTargetEntity.hasPrimaryProviderId()
                ? entitiesSnapshot.getEntityFromOid(optTargetEntity.getPrimaryProviderId()).orElse(null)
                : null;
        // if we do have current provider then add that to the action description
        if (primaryProviderEntity != null) {
            description = new StringBuilder(ActionMessageFormat.ACTION_DESCRIPTION_SCALE_COMMODITY_CHANGE_WITH_CURRENT_PROVIDER.format(
                    firstResizeInfo.getNewCapacity() > firstResizeInfo.getOldCapacity() ? UP : DOWN,
                    commodityTypeDisplayName,
                    beautifyEntityTypeAndName(optTargetEntity),
                    primaryProviderEntity.getDisplayName(),
                    formatResizeActionCommodityValue(firstResizeInfoCommodityType,
                            optTargetEntity.getEntityType(), firstResizeInfo.getOldCapacity(),
                            false),
                    formatResizeActionCommodityValue(firstResizeInfoCommodityType,
                            optTargetEntity.getEntityType(), firstResizeInfo.getNewCapacity(),
                            true)));
        } else {
            description = new StringBuilder(ActionMessageFormat.ACTION_DESCRIPTION_SCALE_COMMODITY_CHANGE.format(
                    firstResizeInfo.getNewCapacity() > firstResizeInfo.getOldCapacity() ? UP : DOWN,
                    commodityTypeDisplayName, beautifyEntityTypeAndName(optTargetEntity),
                    formatResizeActionCommodityValue(firstResizeInfoCommodityType,
                            optTargetEntity.getEntityType(), firstResizeInfo.getOldCapacity(),
                            false),
                    formatResizeActionCommodityValue(firstResizeInfoCommodityType,
                            optTargetEntity.getEntityType(), firstResizeInfo.getNewCapacity(),
                            true)));
        }

        // Scaling more than one commodity
        description.append(getAdditionalResizeCommodityMessage(resizeInfos, optTargetEntity, optTargetEntity));
        return getDescriptionWithAccountName(entitiesSnapshot, targetEntityId,
                description.toString());
    }

    /**
     * Gets names of source and destination regions for cloud migration actions. These names are
     * now used in the final action description text, instead of the compute/storage tier names.
     * Change only being done for cloud migration move actions, all other descriptions remain same.
     *
     * @param action Action whose change providers are checked to look for regions.
     * @param entitiesSnapshot Region names are looked up in the snapshot map.
     * @return Pair with first value (can be null for onPrem) for source region/zone name, second
     *       value for destination region name.
     */
    @Nonnull
    private static Pair<String, String> getRegions(@Nonnull final ActionDTO.Action action,
            @Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot) {
        // source can be zone or region in case of cloud, not set for onPrem.
        String sourceLocation = null;
        String destinationRegion = null;
        if (action.getInfo().getActionTypeCase() != ActionTypeCase.MOVE) {
            return new Pair<>(sourceLocation, destinationRegion);
        }
        // Find the change provider first that has the destination region.
        final Optional<ChangeProvider> regionChangeProvider = action.getInfo().getMove()
                .getChangesList()
                .stream()
                .filter(cp -> cp.hasDestination()
                        && cp.getDestination().getType() == EntityType.REGION_VALUE)
                .findFirst();
        if (!regionChangeProvider.isPresent()) {
            return new Pair<>(sourceLocation, destinationRegion);
        }
        final Optional<ActionPartialEntity> destinationEntity = entitiesSnapshot.getEntityFromOid(
                regionChangeProvider.get().getDestination().getId());
        if (destinationEntity.isPresent()) {
            destinationRegion = destinationEntity.get().getDisplayName();
        }
        if (regionChangeProvider.get().hasSource()) {
            final Optional<ActionPartialEntity> sourceEntity = entitiesSnapshot.getEntityFromOid(
                    regionChangeProvider.get().getSource().getId());
            if (sourceEntity.isPresent()) {
                sourceLocation = sourceEntity.get().getDisplayName();
            }
        }
        return new Pair<>(sourceLocation, destinationRegion);
    }

    /**
     * Builds the RI Buy action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @return The RI Buy action description.
     */
    private static String getRIBuyActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                             @Nonnull final ActionDTO.Action recommendation)  {
        BuyRI buyRI = recommendation.getInfo().getBuyRi();
        String count = String.valueOf(buyRI.getCount());
        long computeTierId =  buyRI.getComputeTier().getId();
        if (!entitiesSnapshot.getEntityFromOid(computeTierId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getRIBuyActionDescription", "computeTierId", computeTierId);
            return "";
        }
        ActionPartialEntity computeTier = entitiesSnapshot.getEntityFromOid(computeTierId).get();
        final String computeTierName = computeTier.getDisplayName();

        long masterAccountId =  buyRI.getMasterAccount().getId();
        if (!entitiesSnapshot.getEntityFromOid(masterAccountId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getRIBuyActionDescription", "masterAccountId", masterAccountId);
            return "";
        }
        ActionPartialEntity masterAccount = entitiesSnapshot.getEntityFromOid(masterAccountId).get();
        final String masterAccountName = masterAccount.getDisplayName();

        long regionId = buyRI.getRegion().getId();
        if (!entitiesSnapshot.getEntityFromOid(regionId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getRIBuyActionDescription", "regionId", regionId);
            return "";
        }
        ActionPartialEntity region = entitiesSnapshot.getEntityFromOid(regionId).get();
        final String regionName = region.getDisplayName();

        if (buyRI.hasTargetEntity()) {
            // If the BuyRI has a virtual machine then this is a BuyRI action from the MPC plan. We want to show the
            // virtual machine name in the action description instead of the account name: OM-62857
            long targetEntityId = buyRI.getTargetEntity().getId();
            if (!entitiesSnapshot.getEntityFromOid(targetEntityId).isPresent()) {
                logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getRIBuyActionDescription", "targetEntityId", targetEntityId);
                return "";
            }
            ActionPartialEntity targetEntity = entitiesSnapshot.getEntityFromOid(targetEntityId).get();
            final String targetEntityName = targetEntity.getDisplayName();

            // Return the description with the virtual machine name instead of the master account name
            return ActionMessageFormat.ACTION_DESCRIPTION_BUYRI.format(count, computeTierName,
                    targetEntityName, regionName);
        }

        return ActionMessageFormat.ACTION_DESCRIPTION_BUYRI.format(count, computeTierName,
            masterAccountName, regionName);
    }

    /**
     * Builds the Provision action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * e.g. ProvisionBySupply action description:
     *      Provision Storage storage_source_test
     * e.g. ProvisionByDemand action description:
     *      Provision Physical Machine similar to pm_source_test with scaled up Mem due to vm1_test
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @return The Provision action description.
     */
    private static String getProvisionActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                        @Nonnull final ActionDTO.Action recommendation) {
        long entityId = recommendation.getInfo().getProvision().getEntityToClone().getId();
        if (!entitiesSnapshot.getEntityFromOid(entityId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getProvisionActionDescription", "entityId", entityId);
            return "";
        }

        final ProvisionExplanation explanation = recommendation.getExplanation().getProvision();
        final ActionPartialEntity entityDTO = entitiesSnapshot.getEntityFromOid(entityId).get();
        if (explanation.getProvisionExplanationTypeCase().getNumber() ==
                        ProvisionExplanation.PROVISION_BY_SUPPLY_EXPLANATION_FIELD_NUMBER) {
            String provisionedEntity = new StringBuilder()
                 .append(ApiEntityType.fromType(entityDTO.getEntityType()).displayName())
                 .append(" similar to ").append(entityDTO.getDisplayName())
                 .toString();
            ActionMessageFormat messageFormat = hasLicense(explanation.getReconfigCommTypesList())
                ? ActionMessageFormat.ACTION_DESCRIPTION_LICENSED_PROVISION_BY_SUPPLY
                : ActionMessageFormat.ACTION_DESCRIPTION_PROVISION_BY_SUPPLY;
            return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                    messageFormat.format(provisionedEntity, getProvisionedForVSANSuffix(entityDTO,
                            explanation.getProvisionBySupplyExplanation(), entitiesSnapshot)));
        } else {
            final ProvisionByDemandExplanation provisionByDemandExplanation =
                explanation.getProvisionByDemandExplanation();
            ActionMessageFormat messageFormat = hasLicense(explanation.getReconfigCommTypesList())
                    ? ActionMessageFormat.ACTION_DESCRIPTION_LICENSED_PROVISION_BY_DEMAND
                    : ActionMessageFormat.ACTION_DESCRIPTION_PROVISION_BY_DEMAND;
            return getDescriptionWithAccountName(entitiesSnapshot, entityId, messageFormat.format(
                    StringUtil.beautifyString(
                            EntityType.forNumber(entityDTO.getEntityType()).name()),
                    entityDTO.getDisplayName(),
                    // Beautify all reason commodities.
                    beautifyCommodityTypes(
                            provisionByDemandExplanation.getCommodityNewCapacityEntryList()
                                    .stream()
                                    .map(CommodityNewCapacityEntry::getCommodityBaseType)
                                    .map(baseType -> TopologyDTO.CommodityType.newBuilder()
                                            .setType(baseType)
                                            .build())
                                    .collect(Collectors.toList())),
                    entitiesSnapshot.getEntityFromOid(provisionByDemandExplanation.getBuyerId())
                            .map(ActionPartialEntity::getDisplayName)
                            .orElse("high demand")));
        }
    }

    /**
     * Check whether scaling up a vSAN storage is the reason and if yes then make a suffix for description.
     * @param entityDTO the entity for which the description is created.
     * @param explanation   action explanation.
     * @param entitiesSnapshot  all the entities for which actions have been generated.
     * @return  suffix with the name of vSAN or empty string if this has nothing to do with vSAN.
     */
    private static String getProvisionedForVSANSuffix(@Nonnull ActionPartialEntity entityDTO,
                    @Nonnull ProvisionBySupplyExplanation explanation,
                    @Nonnull EntitiesAndSettingsSnapshot entitiesSnapshot)  {
        if (entityDTO.getEntityType() != EntityType.PHYSICAL_MACHINE_VALUE
                        || !explanation.hasMostExpensiveCommodityInfo()
                        || !explanation.getMostExpensiveCommodityInfo().hasCommodityType()
                        || !explanation.getMostExpensiveCommodityInfo().getCommodityType().hasType()
                        || entityDTO.getConnectedEntitiesCount() == 0) {
            return "";
        }
        int actionCommodityType = explanation.getMostExpensiveCommodityInfo().getCommodityType().getType();
        if (!HCIUtils.isVSANRelatedCommodity(actionCommodityType)) {
            return "";
        }
        for (ConnectedEntity connected : entityDTO.getConnectedEntitiesList())  {
            if (connected.hasConnectedEntityType() && connected.hasConnectedEntityId()
                            && connected.getConnectedEntityType() == EntityType.STORAGE_VALUE) {
                Optional<ActionPartialEntity> optionalEntity = entitiesSnapshot
                                .getEntityFromOid(connected.getConnectedEntityId());
                if (isVSAN(optionalEntity))    {
                    return " to scale Storage " + optionalEntity.get().getDisplayName();
                }
            }
        }
        return "";
    }

    /**
     * Checks whether Optional<{@link ActionPartialEntity}> is a vSAN entity.
     * @param optionalEntity    the entity we check
     * @return true if is vSAN
     */
    private static boolean isVSAN(Optional<ActionPartialEntity> optionalEntity) {
        return optionalEntity.isPresent() && optionalEntity.get().hasTypeSpecificInfo()
                        && optionalEntity.get().getTypeSpecificInfo().hasStorage()
                        && optionalEntity.get().getTypeSpecificInfo().getStorage().hasStorageType()
                        && optionalEntity.get().getTypeSpecificInfo().getStorage().getStorageType()
                            == StorageType.VSAN;
    }

    /**
     * Builds the Delete action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information specific to detached volumes.
     * @param recommendation {@link ActionDTO.Action} the action object
     * @return The Delete action description.
     */
    private static String getDeleteActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                     @Nonnull final ActionDTO.Action recommendation)  {

        int targetEntityType = recommendation.getInfo().getDelete().getTarget().getType();
        long targetEntityId = recommendation.getInfo().getDelete().getTarget().getId();
        if (!entitiesSnapshot.getEntityFromOid(targetEntityId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getDeleteActionDescription", "targetEntityId",
                    targetEntityId);
            return "";
        }

        // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
        //  can remove APPLICATION_COMPONENT_VALUE when legacy support not needed
        // If it got to delete stage as an app component or vm spec, it's an App Service Plan.
        if (targetEntityType == EntityType.VIRTUAL_MACHINE_SPEC_VALUE ||
                targetEntityType == EntityType.APPLICATION_COMPONENT_VALUE) {
            ActionPartialEntity targetEntity = entitiesSnapshot.getEntityFromOid(targetEntityId)
                    .get();
            long sourceEntityId = recommendation.getInfo().getDelete().getSource().getId();
            final Optional<EntityWithConnections> businessAccountTopologyEntityOpt =
                    entitiesSnapshot.getOwnerAccountOfEntity(targetEntityId);
            if (entitiesSnapshot.getEntityFromOid(sourceEntityId).isPresent()
                    && businessAccountTopologyEntityOpt.isPresent()) {
                String tier = entitiesSnapshot.getEntityFromOid(sourceEntityId)
                        .get()
                        .getDisplayName();
                String subscription = businessAccountTopologyEntityOpt.get().getDisplayName();
                return ActionMessageFormat.ACTION_DESCRIPTION_DELETE_APPLICATION_SERVICE.format(
                        tier, targetEntity.getDisplayName(), subscription);
            } else {
                // Default to generic delete message for ASP in case we don't have subscription or family.
                return ActionMessageFormat.ACTION_DESCRIPTION_DELETE_APPLICATION_SERVICE_DEFAULT.format(
                        targetEntity.getDisplayName());
            }
        }

        // Handle volumes
        if (recommendation.getInfo().getDelete().getTarget().getEnvironmentType() == EnvironmentType.CLOUD) {
            ActionPartialEntity targetEntity = entitiesSnapshot.getEntityFromOid(targetEntityId).get();

            final Optional<EntityWithConnections> businessAccountTopologyEntityOpt =
                entitiesSnapshot.getOwnerAccountOfEntity(targetEntityId);
            String sourceDisplayName = "";

            if (recommendation.getInfo().getDelete().hasSource()) {
                long sourceEntityId = recommendation.getInfo().getDelete().getSource().getId();

                Optional<ActionPartialEntity> sourceEntityOpt = entitiesSnapshot.getEntityFromOid(sourceEntityId);
                if (sourceEntityOpt.isPresent()) {
                    sourceDisplayName = sourceEntityOpt.get().getDisplayName();
                } else {
                    logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getDeleteActionDescription", "sourceEntityId", sourceEntityId);
                }

            } else {
                logger.error("No source entity for the DELETE action {} for volume {}",
                    recommendation.getId(),
                    targetEntityId);
            }

            if (businessAccountTopologyEntityOpt.isPresent() && !Strings.isNullOrEmpty(businessAccountTopologyEntityOpt.get().getDisplayName())) {
                return ActionMessageFormat.ACTION_DESCRIPTION_DELETE_CLOUD.format(
                    sourceDisplayName,
                    targetEntity.getDisplayName(),
                    businessAccountTopologyEntityOpt.get().getDisplayName());
            } else {
                logger.warn("Unable to get Business Account Name from repository for Virtual Volume Oid: {}", targetEntityId);
                return ActionMessageFormat.ACTION_DESCRIPTION_DELETE_CLOUD_NO_ACCOUNT.format(
                    sourceDisplayName,
                    targetEntity.getDisplayName());
            }
        } else {
            String deleteFilePath = recommendation.getInfo().getDelete().getFilePath();

            return ActionMessageFormat.ACTION_DESCRIPTION_DELETE.format(
                deleteFilePath.substring(deleteFilePath.lastIndexOf('/') + 1),
                beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(
                    recommendation.getInfo().getDelete().getTarget().getId()).get()),
                FileUtils.byteCountToDisplaySize(
                    recommendation.getExplanation().getDelete().getSizeKb()
                        * FileUtils.ONE_KB));
        }
    }

    /**
     * Builds the Activate action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @return The Activate action description.
     */
    private static String getActivateActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                     @Nonnull final ActionDTO.Action recommendation) {
        long entityId = recommendation.getInfo().getActivate().getTarget().getId();
        if (!entitiesSnapshot.getEntityFromOid(entityId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getActivateActionDescription", "entityId", entityId);
            return "";
        }
        return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                ActionMessageFormat.ACTION_DESCRIPTION_ACTIVATE.format(beautifyEntityTypeAndName(
                        entitiesSnapshot.getEntityFromOid(entityId).get())));
    }

    /**
     * Builds the Deactivate action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @return The Deactivate action description.
     */
    private static String getDeactivateActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                       @Nonnull final ActionDTO.Action recommendation) {
        long entityId = recommendation.getInfo().getDeactivate().getTarget().getId();
        if (!entitiesSnapshot.getEntityFromOid(entityId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getDeactivateActionDescription", "entityId", entityId);
            return "";
        }
        DeactivateExplanation explanation = recommendation.getExplanation().getDeactivate();
        ActionMessageFormat messageFormat = hasLicense(explanation.getReconfigCommTypesList())
            ? ActionMessageFormat.ACTION_DESCRIPTION_LICENSED_DEACTIVATE
            : ActionMessageFormat.ACTION_DESCRIPTION_DEACTIVATE;
        return getDescriptionWithAccountName(entitiesSnapshot, entityId,
                messageFormat.format(StringUtil.beautifyString(ActionDTO.ActionType.SUSPEND.name()),
                        beautifyEntityTypeAndName(
                                entitiesSnapshot.getEntityFromOid(entityId).get())));
    }

    /**
     * Format resize actions commodity capacity value to more readable format.
     *
     * @param commodityType commodity type.
     * @param entityType the entity type to be checked
     * @param capacity commodity capacity which needs to format.
     * @param isNewCapacity
     * @return a string after format.
     */
    static String formatResizeActionCommodityValue(
            @Nonnull final CommodityType commodityType, final int entityType, final double capacity,
            boolean isNewCapacity) {
        // Convert capacity of the commodities in this map into human readable format and round the number
        // with 1 significant figure in decimal, eg. 100.2 MB, 11.0 GB, etc.
        if (commodityTypeToDefaultUnits.containsKey(commodityType)) {
            long capacityInBytes = (long)(capacity * commodityTypeToDefaultUnits.get(commodityType) / Units.BYTE);
            return StringUtil.getHumanReadableSize(capacityInBytes);
        } else if (CLOUD_NATIVE_ENTITIES.contains(entityType)) {
            return ActionMessageFormat.CONTAINER_VCPU_MILLICORES.format(capacity);
        } else if (COMMODITY_TYPE_ACTION_MESSAGE_FORMAT_MAP.containsKey(commodityType)) {
            // Convert IO_Throughput value from KB/s to MB/s in action description
            double adaptCapacityToUnit = CommodityType.IO_THROUGHPUT == commodityType ? capacity / Units.KIBI : capacity;
            return COMMODITY_TYPE_ACTION_MESSAGE_FORMAT_MAP.get(commodityType).format(adaptCapacityToUnit);
        } else if(commodityType == CommodityType.VCPU && isNewCapacity){
            return ActionMessageFormat.VCPU_CORES.format(capacity);
        } else {
            return ActionMessageFormat.SIMPLE.format(capacity);
        }
    }

    /**
     * Checks if the List of commodity types includes a license commodity.
     *
     * @param commodities List of commodities to check.
     *
     * @return true if the list contains a license commodity. False if it doesn't.
     */
    private static boolean hasLicense(List<TopologyDTO.CommodityType> commodities) {
        return commodities.stream()
            .map(TopologyDTO.CommodityType::getType)
            .anyMatch(type -> type == CommodityType.SOFTWARE_LICENSE_COMMODITY_VALUE);
    }

    /**
     * Returns description with account name for the action.
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains
     *         entities
     *         information.
     * @param targetId the target entity id.
     * @param description action description without account name.
     * @return The action description with account name.
     */
    private static String getDescriptionWithAccountName(final EntitiesAndSettingsSnapshot entitiesSnapshot,
            final Long targetId, String description) {
        final Optional<EntityWithConnections> businessAccount = entitiesSnapshot.getOwnerAccountOfEntity(
                targetId);
        if (businessAccount.isPresent() && !Strings.isNullOrEmpty(businessAccount.get().getDisplayName())) {
            return description.concat(ActionMessageFormat.ACTION_DESCRIPTION_ACCOUNT_NAME.format(
                    businessAccount.get().getDisplayName()));
        } else {
            logger.debug("Cannot find Business Account for target Id. ID - {})", targetId);
            return description;
        }
    }
}
