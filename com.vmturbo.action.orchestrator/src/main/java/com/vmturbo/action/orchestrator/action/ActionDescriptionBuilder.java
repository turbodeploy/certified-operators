package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyCommodityTypes;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyEntityTypeAndName;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyString;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityNewCapacityEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ActionDescriptionBuilder {

    private static final Logger logger = LogManager.getLogger();
    private static final String ACTION_DESCRIPTION_PROVISION_BY_SUPPLY = "Provision {0}";
    private static final String ACTION_DESCRIPTION_PROVISION_BY_DEMAND =
        "Provision {0} similar to {1} with scaled up {2} due to {3}";
    private static final String ACTION_DESCRIPTION_ACTIVATE =
        "Start {0} due to increased demand for resources";
    private static final String ACTION_DESCRIPTION_DEACTIVATE = "{0} {1}";
    private static final String ACTION_DESCRIPTION_DELETE =
        "Delete wasted file ''{0}'' from {1} to free up {2}";
    private static final String ACTION_DESCRIPTION_RESIZE_REMOVE_LIMIT =
        "Remove {0} limit on entity {1}";
    private static final String ACTION_DESCRIPTION_RESIZE = "Resize {0} {1} for {2} from {3} to {4}";
    private static final String ACTION_DESCRIPTION_RECONFIGURE_WITH_SOURCE =
        "Reconfigure {0} which requires {1} but is hosted by {2} which does not provide {1}";
    private static final String ACTION_DESCRIPTION_RECONFIGURE = "Reconfigure {0} as it is unplaced";
    private static final String ACTION_DESCRIPTION_MOVE_WITHOUT_SOURCE = "Start {0} on {1}";
    private static final String ACTION_DESCRIPTION_MOVE = "{0} {1}{2} from {3} to {4}";
    private static final String ACTION_DESCRIPTION_BUYRI = "Buy {0} {1} RIs for {2} in {3}";
    private static final String MOVE = "Move";
    private static final String SCALE = "Scale";
    private static final String UP = "up";
    private static final String DOWN = "down";
    private static final String OF = " of ";
    private static final String DEFAULT_ERROR_MSG = "Unsupported action type ";
    private static final String ENTITY_NOT_FOUND_WARN_MSG =
        "Entity {0} doesn't exist in the entities snapshot";

    // Commodities in actions mapped to their default units.
    // For example, vMem commodity has its default capacity unit as KB.
    private static final ImmutableMap<CommodityDTO.CommodityType, Long> commodityTypeToDefaultUnits =
        new ImmutableMap.Builder<CommodityDTO.CommodityType, Long>()
            .put(CommodityDTO.CommodityType.VMEM, Units.KBYTE)
            .put(CommodityDTO.CommodityType.STORAGE_AMOUNT, Units.MBYTE)
            .put(CommodityDTO.CommodityType.HEAP, Units.KBYTE)
            .put(CommodityDTO.CommodityType.MEM, Units.KBYTE)
            .build();

    /**
     * Builds the action description to be passed to the API component. Forming the description was
     * done initially in the API component, but it was moved to AO to take advantage of
     * pagination.
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @return The final action description string that will be displayed in the UI.
     */
    public static String buildActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                @Nonnull final ActionDTO.Action recommendation) {
        final ActionInfo info = recommendation.getInfo();
        switch(info.getActionTypeCase()) {
            case MOVE:
                return getMoveActionDescription(entitiesSnapshot, recommendation);
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
                throw new IllegalArgumentException(DEFAULT_ERROR_MSG +
                    info.getActionTypeCase().name());
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
        Long entityId = resize.getTarget().getId();
        if (!entitiesSnapshot.getEntityFromOid(entityId).isPresent()) {
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, entityId));
            return "";
        }
        // Check if we need to describe the action as a "remove limit" instead of regular resize.
        if (resize.getCommodityAttribute() == CommodityAttribute.LIMIT) {
            return MessageFormat.format(ACTION_DESCRIPTION_RESIZE_REMOVE_LIMIT,
                beautifyCommodityTypes(Collections.singletonList(
                    recommendation.getInfo().getResize().getCommodityType())),
                beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(entityId).get()));
        } else {
            // Regular case
            final CommodityDTO.CommodityType commodityType = CommodityDTO.CommodityType
                .forNumber(resize.getCommodityType().getType());
            return MessageFormat.format(ACTION_DESCRIPTION_RESIZE,
                resize.getNewCapacity() > resize.getOldCapacity() ? UP : DOWN,
                beautifyCommodityTypes(Collections.singletonList(resize.getCommodityType())),
                beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(entityId).get()),
                formatResizeActionCommodityValue(commodityType, resize.getOldCapacity()),
                formatResizeActionCommodityValue(commodityType, resize.getNewCapacity()));
        }
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
        final Long entityId = reconfigure.getTarget().getId();
        if (!entitiesSnapshot.getEntityFromOid(entityId).isPresent()) {
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, entityId));
            return "";
        }
        if (reconfigure.hasSource()) {
            long sourceId = reconfigure.getSource().getId();
            Optional<ActionPartialEntity> currentEntityDTO = entitiesSnapshot.getEntityFromOid(
                sourceId);
            if (!currentEntityDTO.isPresent()) {
                logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, sourceId));
                return "";
            }
            return MessageFormat.format(
                ACTION_DESCRIPTION_RECONFIGURE_WITH_SOURCE,
                beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(entityId).get()),
                beautifyCommodityTypes(recommendation.getExplanation().getReconfigure()
                    .getReconfigureCommodityList().stream()
                    .map(ReasonCommodity::getCommodityType)
                    .collect(Collectors.toList())),
                beautifyEntityTypeAndName(currentEntityDTO.get()));
        } else {
            return MessageFormat.format(
                ACTION_DESCRIPTION_RECONFIGURE,
                beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(entityId).get()));
        }
    }

    /**
     * Builds the Move action description. This is intended to be called by
     * {@link ActionDescriptionBuilder#buildActionDescription(EntitiesAndSettingsSnapshot, ActionDTO.Action)}
     *
     * @param entitiesSnapshot {@link EntitiesAndSettingsSnapshot} object that contains entities
     * information.
     * @return The Move action description.
     */
    private static String getMoveActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                            @Nonnull final ActionDTO.Action recommendation) {
        final boolean initialPlacement =
            recommendation.getExplanation().getMove().getChangeProviderExplanationList()
                .stream().anyMatch(ChangeProviderExplanation::hasInitialPlacement);
        final Move move = recommendation.getInfo().getMove();
        ChangeProvider primaryChange = ActionDTOUtil
            .getPrimaryChangeProvider(move);

        final boolean hasSource = !initialPlacement && primaryChange.hasSource();
        final long destinationEntityId = primaryChange.getDestination().getId();
        final long targetEntityId = move.getTarget().getId();

        if( !entitiesSnapshot.getEntityFromOid(targetEntityId).isPresent()) {
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, targetEntityId));
            return "";
        }
        if( !entitiesSnapshot.getEntityFromOid(destinationEntityId).isPresent()) {
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, destinationEntityId));
            return "";
        }
        // All moves should have a target entity and a destination.
        ActionPartialEntity targetEntityDTO = entitiesSnapshot.getEntityFromOid(
            targetEntityId).get();

        ActionPartialEntity newEntityDTO = entitiesSnapshot.getEntityFromOid(
            destinationEntityId).get();

        if (!hasSource) {
            return MessageFormat.format(ACTION_DESCRIPTION_MOVE_WITHOUT_SOURCE,
                beautifyEntityTypeAndName(targetEntityDTO),
                beautifyEntityTypeAndName(newEntityDTO));
        } else {
            long sourceEntityId = primaryChange.getSource().getId();
            ActionPartialEntity currentEntityDTO = entitiesSnapshot.getEntityFromOid(
                sourceEntityId).get();
            int sourceType = currentEntityDTO.getEntityType();
            int destinationType = newEntityDTO.getEntityType();
            String verb = TopologyDTOUtil.isTierEntityType(destinationType)
                && TopologyDTOUtil.isTierEntityType(sourceType) ? SCALE : MOVE;
            String resource = "";
            if (primaryChange.hasResource()) {
                Optional<ActionPartialEntity> resourceEntity = entitiesSnapshot.getEntityFromOid(
                    primaryChange.getResource().getId());
                if (resourceEntity.isPresent()) {
                    resource = beautifyEntityTypeAndName(resourceEntity.get()) + OF;
                }
            }
            return MessageFormat.format(ACTION_DESCRIPTION_MOVE, verb, resource,
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
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, computeTierId));
            return "";
        }
        ActionPartialEntity computeTier = entitiesSnapshot.getEntityFromOid(computeTierId).get();
        final String computeTierName = (computeTier != null) ? computeTier.getDisplayName() : "";

        long masterAccountId =  buyRI.getMasterAccount().getId();
        if (!entitiesSnapshot.getEntityFromOid(masterAccountId).isPresent()) {
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, masterAccountId));
            return "";
        }
        ActionPartialEntity masterAccount = entitiesSnapshot.getEntityFromOid(masterAccountId).get();
        final String masterAccountName = (masterAccount != null) ? masterAccount.getDisplayName() : "";

        long regionId = buyRI.getRegionId().getId();
        if (!entitiesSnapshot.getEntityFromOid(regionId).isPresent()) {
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, regionId));
            return "";
        }
        ActionPartialEntity region = entitiesSnapshot.getEntityFromOid(regionId).get();
        final String regionName = (region != null) ? region.getDisplayName() : "";

        return MessageFormat.format(ACTION_DESCRIPTION_BUYRI, count, computeTierName,
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
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, entityId));
            return "";
        }

        final ProvisionExplanation explanation = recommendation.getExplanation().getProvision();
        final ActionPartialEntity entityDTO = entitiesSnapshot.getEntityFromOid(entityId).get();
        if (explanation.getProvisionExplanationTypeCase().getNumber() ==
                        ProvisionExplanation.PROVISION_BY_SUPPLY_EXPLANATION_FIELD_NUMBER) {
            return MessageFormat.format(ACTION_DESCRIPTION_PROVISION_BY_SUPPLY,
                beautifyEntityTypeAndName(entityDTO));
        } else {
            final ProvisionByDemandExplanation provisionByDemandExplanation =
                explanation.getProvisionByDemandExplanation();
            return MessageFormat.format(ACTION_DESCRIPTION_PROVISION_BY_DEMAND,
                beautifyString(EntityType.forNumber(entityDTO.getEntityType()).name()),
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
     * information.
     * @return The Delete action description.
     */
    private static String getDeleteActionDescription(@Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                     @Nonnull final ActionDTO.Action recommendation) {
        // TODO need to give savings in terms of cost instead of file size for Cloud entities
        String deleteFilePath = recommendation.getInfo().getDelete().getFilePath();
        long entityId = recommendation.getInfo().getDelete().getTarget().getId();
        if (!entitiesSnapshot.getEntityFromOid(entityId).isPresent()) {
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, entityId));
            return "";
        }
        return MessageFormat.format(ACTION_DESCRIPTION_DELETE,
            deleteFilePath.substring(deleteFilePath.lastIndexOf('/') + 1),
            beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(
                recommendation.getInfo().getDelete().getTarget().getId()).get()),
            FileUtils.byteCountToDisplaySize(
                recommendation.getExplanation().getDelete().getSizeKb()
                    * FileUtils.ONE_KB));
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
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, entityId));
            return "";
        }
        return MessageFormat.format(
            ACTION_DESCRIPTION_ACTIVATE,
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
            logger.debug(MessageFormat.format(ENTITY_NOT_FOUND_WARN_MSG, entityId));
            return "";
        }
        return MessageFormat.format(ACTION_DESCRIPTION_DEACTIVATE,
            beautifyString(ActionDTO.ActionType.SUSPEND.name()),
            beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(
                entityId).get()));
    }

    /**
     * Format resize actions commodity capacity value to more readable format.
     *
     * @param commodityType commodity type.
     * @param capacity commodity capacity which needs to format.
     * @return a string after format.
     */
    private static String formatResizeActionCommodityValue(@Nonnull final CommodityDTO.CommodityType commodityType,
                                                    final double capacity) {
        // Currently all items in this map are converted from default units to GB.
        if (commodityTypeToDefaultUnits.keySet().contains(commodityType)) {
            return MessageFormat.format("{0} GB",
                capacity / (Units.GBYTE / commodityTypeToDefaultUnits.get(commodityType)));
        } else {
            return MessageFormat.format("{0}", capacity);
        }
    }
}
