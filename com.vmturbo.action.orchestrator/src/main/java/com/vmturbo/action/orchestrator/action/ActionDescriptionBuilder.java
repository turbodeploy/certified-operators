package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyCommodityType;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyCommodityTypes;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyEntityTypeAndName;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityNewCapacityEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ActionDescriptionBuilder {

    private static final Logger logger = LogManager.getLogger();

    // Static strings for use in message contents.
    private static final String MOVE = "Move";
    private static final String SCALE = "Scale";
    private static final String UP = "up";
    private static final String DOWN = "down";
    private static final String OF = " of ";
    // pass in methodName, name of entity, and entity OID
    private static final String ENTITY_NOT_FOUND_WARN_MSG = "{} {} Entity {} doesn't exist in the entities snapshot";


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
        ACTION_DESCRIPTION_PROVISION_BY_SUPPLY("Provision {0}"),
        ACTION_DESCRIPTION_PROVISION_BY_DEMAND("Provision {0} similar to {1} with scaled up {2} due to {3}"),
        ACTION_DESCRIPTION_ACTIVATE("Start {0} due to increased demand for resources"),
        ACTION_DESCRIPTION_DEACTIVATE("{0} {1}"),
        ACTION_DESCRIPTION_DELETE("Delete wasted file ''{0}'' from {1} to free up {2}"),
        ACTION_DESCRIPTION_DELETE_CLOUD_NO_ACCOUNT("Delete Unattached {0} Volume {1}"),
        ACTION_DESCRIPTION_DELETE_CLOUD("Delete Unattached {0} Volume {1} from {2}"),
        ACTION_DESCRIPTION_RESIZE_REMOVE_LIMIT("Remove {0} limit on entity {1}"),
        ACTION_DESCRIPTION_RESIZE("Resize {0} {1} for {2} from {3} to {4}"),
        ACTION_DESCRIPTION_RESIZE_RESERVATION("Resize {0} {1} reservation for {2} from {3} to {4}"),
        ACTION_DESCRIPTION_RECONFIGURE_REASON_COMMODITIES("Reconfigure {0} which requires {1} but is hosted by {2} which does not provide {1}"),
        ACTION_DESCRIPTION_RECONFIGURE_REASON_SETTINGS("Reconfigure {0}"),
        ACTION_DESCRIPTION_RECONFIGURE_WITHOUT_SOURCE("Reconfigure {0} as it is unplaced"),
        ACTION_DESCRIPTION_MOVE_WITHOUT_SOURCE("Start {0} on {1}"),
        ACTION_DESCRIPTION_MOVE("{0} {1}{2} from {3} to {4}"),
        ACTION_DESCRIPTION_BUYRI("Buy {0} {1} RIs for {2} in {3}"),
        ACTION_DESCRIPTION_ALLOCATE("Increase RI coverage for {0} in {1}"),
        CONTAINER_VCPU_MHZ("{0,number,integer} MHz"),
        SIMPLE_GB("{0, number, integer} GB"),
        SIMPLE("{0, number, integer}");

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
            .put(CommodityDTO.CommodityType.STORAGE_AMOUNT, Units.MBYTE)
            .put(CommodityDTO.CommodityType.STORAGE_PROVISIONED, Units.MBYTE)
            .put(CommodityDTO.CommodityType.HEAP, Units.KBYTE)
            .put(CommodityDTO.CommodityType.MEM, Units.KBYTE)
            .put(CommodityDTO.CommodityType.DB_MEM, Units.KBYTE)
            .put(CommodityDTO.CommodityType.VSTORAGE, Units.MBYTE)
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
            return ActionMessageFormat.ACTION_DESCRIPTION_RESIZE_REMOVE_LIMIT.format(
                beautifyCommodityType(resize.getCommodityType()),
                beautifyEntityTypeAndName(entity));
        }

        ActionMessageFormat messageFormat =
            resize.getCommodityAttribute() == CommodityAttribute.CAPACITY ?
                ActionMessageFormat.ACTION_DESCRIPTION_RESIZE :
                ActionMessageFormat.ACTION_DESCRIPTION_RESIZE_RESERVATION;
        return messageFormat.format(
            resize.getNewCapacity() > resize.getOldCapacity() ? UP : DOWN,
            beautifyCommodityType(resize.getCommodityType()),
            beautifyEntityTypeAndName(entity),
            formatResizeActionCommodityValue(
                commodityType, entity.getEntityType(), resize.getOldCapacity()),
            formatResizeActionCommodityValue(
                commodityType, entity.getEntityType(), resize.getNewCapacity()));
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
        final Reconfigure reconfigure = recommendation.getInfo().getReconfigure();
        final long entityId = reconfigure.getTarget().getId();
        Optional<ActionPartialEntity> targetEntityDTO = entitiesSnapshot.getEntityFromOid(entityId);
        if (!targetEntityDTO.isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getReconfigureActionDescription", "entityId", entityId);
            return "";
        }
        if (reconfigure.hasSource()) {
            long sourceId = reconfigure.getSource().getId();
            Optional<ActionPartialEntity> currentEntityDTO = entitiesSnapshot.getEntityFromOid(
                sourceId);
            if (!currentEntityDTO.isPresent()) {
                logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getReconfigureActionDescription", "sourceId", sourceId);
                return "";
            }
            Explanation explanation = recommendation.getExplanation();
            if (!explanation.getReconfigure().getReasonSettingsList().isEmpty()) {
                return ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_REASON_SETTINGS.format(
                    beautifyEntityTypeAndName(targetEntityDTO.get()));
            } else {
                return ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_REASON_COMMODITIES.format(
                    beautifyEntityTypeAndName(targetEntityDTO.get()),
                    beautifyCommodityTypes(explanation.getReconfigure()
                        .getReconfigureCommodityList().stream()
                        .map(ReasonCommodity::getCommodityType)
                        .collect(Collectors.toList())),
                    beautifyEntityTypeAndName(currentEntityDTO.get()));
            }
        } else {
            return ActionMessageFormat.ACTION_DESCRIPTION_RECONFIGURE_WITHOUT_SOURCE.format(
                beautifyEntityTypeAndName(targetEntityDTO.get()));
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
                beautifyEntityTypeAndName(workloadEntity), accountName);
    }

    /**
     * Builds Move or Right Size action description. This is intended to be called by
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
        final boolean initialPlacement = ActionDTOUtil.getChangeProviderExplanationList(
            recommendation.getExplanation()).stream()
            .anyMatch(ChangeProviderExplanation::hasInitialPlacement);

        final ChangeProvider primaryChange = ActionDTOUtil.getPrimaryChangeProvider(recommendation);
        final boolean hasSource = !initialPlacement && primaryChange.hasSource();
        final long destinationEntityId = primaryChange.getDestination().getId();
        final long targetEntityId = ActionDTOUtil.getPrimaryEntity(recommendation, true).getId();
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
            return ActionMessageFormat.ACTION_DESCRIPTION_MOVE_WITHOUT_SOURCE.format(
                beautifyEntityTypeAndName(targetEntityDTO),
                beautifyEntityTypeAndName(newEntityDTO));
        } else {
            long sourceEntityId = primaryChange.getSource().getId();
            ActionPartialEntity currentEntityDTO = entitiesSnapshot.getEntityFromOid(
                sourceEntityId).get();
            int sourceType = currentEntityDTO.getEntityType();
            int destinationType = newEntityDTO.getEntityType();
            String verb = TopologyDTOUtil.isPrimaryTierEntityType(destinationType)
                && TopologyDTOUtil.isPrimaryTierEntityType(sourceType) ? SCALE : MOVE;
            String resource = "";
            if (primaryChange.hasResource() &&
                    targetEntityId != primaryChange.getResource().getId()) {
                Optional<ActionPartialEntity> resourceEntity = entitiesSnapshot.getEntityFromOid(
                    primaryChange.getResource().getId());
                if (resourceEntity.isPresent()) {
                    resource = beautifyEntityTypeAndName(resourceEntity.get()) + OF;
                }
            }
            return ActionMessageFormat.ACTION_DESCRIPTION_MOVE.format(verb, resource,
                beautifyEntityTypeAndName(targetEntityDTO),
                currentEntityDTO.getDisplayName(),
                newEntityDTO.getDisplayName());
        }
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
        final String computeTierName = (computeTier != null) ? computeTier.getDisplayName() : "";

        long masterAccountId =  buyRI.getMasterAccount().getId();
        if (!entitiesSnapshot.getEntityFromOid(masterAccountId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getRIBuyActionDescription", "masterAccountId", masterAccountId);
            return "";
        }
        ActionPartialEntity masterAccount = entitiesSnapshot.getEntityFromOid(masterAccountId).get();
        final String masterAccountName = (masterAccount != null) ? masterAccount.getDisplayName() : "";

        long regionId = buyRI.getRegion().getId();
        if (!entitiesSnapshot.getEntityFromOid(regionId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getRIBuyActionDescription", "regionId", regionId);
            return "";
        }
        ActionPartialEntity region = entitiesSnapshot.getEntityFromOid(regionId).get();
        final String regionName = (region != null) ? region.getDisplayName() : "";

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
            return ActionMessageFormat.ACTION_DESCRIPTION_PROVISION_BY_SUPPLY.format(
                beautifyEntityTypeAndName(entityDTO));
        } else {
            final ProvisionByDemandExplanation provisionByDemandExplanation =
                explanation.getProvisionByDemandExplanation();
            return ActionMessageFormat.ACTION_DESCRIPTION_PROVISION_BY_DEMAND.format(
                StringUtil.beautifyString(EntityType.forNumber(entityDTO.getEntityType()).name()),
                entityDTO.getDisplayName(),
                // Beautify all reason commodities.
                beautifyCommodityTypes(provisionByDemandExplanation.getCommodityNewCapacityEntryList()
                    .stream().map(CommodityNewCapacityEntry::getCommodityBaseType)
                    .map(baseType -> TopologyDTO.CommodityType.newBuilder().setType(baseType).build())
                    .collect(Collectors.toList())),
                entitiesSnapshot.getEntityFromOid(provisionByDemandExplanation.getBuyerId())
                    .map(ActionPartialEntity::getDisplayName).orElse("high demand"));
        }
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

        long targetEntityId = recommendation.getInfo().getDelete().getTarget().getId();
        if (!entitiesSnapshot.getEntityFromOid(targetEntityId).isPresent()) {
            logger.warn(ENTITY_NOT_FOUND_WARN_MSG, "getDeleteActionDescription", "targetEntityId", targetEntityId);
            return "";
        }

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
        return ActionMessageFormat.ACTION_DESCRIPTION_ACTIVATE.format(
            beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(entityId).get()));
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
        return ActionMessageFormat.ACTION_DESCRIPTION_DEACTIVATE.format(
            StringUtil.beautifyString(ActionDTO.ActionType.SUSPEND.name()),
            beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(
                entityId).get()));
    }

    /**
     * Format resize actions commodity capacity value to more readable format.
     *
     * @param commodityType commodity type.
     * @param entityType the entity type to be checked
     * @param capacity commodity capacity which needs to format.
     * @return a string after format.
     */
    private static String formatResizeActionCommodityValue(
            @Nonnull final CommodityDTO.CommodityType commodityType,
            final int entityType, final double capacity) {
        // Currently all items in this map are converted from default units to GB.
        if (commodityTypeToDefaultUnits.containsKey(commodityType)) {
            return ActionMessageFormat.SIMPLE_GB.format(
                capacity / (Units.GBYTE / commodityTypeToDefaultUnits.get(commodityType)));
        } else if (entityType == EntityType.CONTAINER_VALUE) {
            return ActionMessageFormat.CONTAINER_VCPU_MHZ.format(capacity);
        } else {
            return ActionMessageFormat.SIMPLE.format(capacity);
        }
    }
}
