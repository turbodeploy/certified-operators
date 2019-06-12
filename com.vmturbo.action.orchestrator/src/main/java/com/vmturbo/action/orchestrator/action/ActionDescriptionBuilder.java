package com.vmturbo.action.orchestrator.action;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.WordUtils;
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
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ActionDescriptionBuilder {

    private static final Logger logger = LogManager.getLogger();
    private static final String ACTION_DESCRIPTION_PROVISION = "Provision {0}";
    private static final String ACTION_DESCRIPTION_ACTIVATE = "Start {0} due to increased demand for resources";
    private static final String ACTION_DESCRIPTION_DEACTIVATE = "{0} {1}";
    private static final String ACTION_DESCRIPTION_DELETE = "Delete wasted file ''{0}'' from {1} to free up {2}";
    private static final String ACTION_DESCRIPTION_RESIZE_REMOVE_LIMIT = "Remove {0} limit on entity {1}";
    private static final String ACTION_DESCRIPTION_RESIZE = "Resize {0} {1} for {2} from {3} to {4}";
    private static final String ACTION_DESCRIPTION_RECONFIGURE_WITH_SOURCE = "Reconfigure {0} which requires {1} but is hosted by {2} which does not provide {1}";
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

    private static final Set<String> TIER_VALUES = ImmutableSet.of(
        UIEntityType.COMPUTE_TIER.apiStr(), UIEntityType.DATABASE_SERVER_TIER.apiStr(),
        UIEntityType.DATABASE_TIER.apiStr(), UIEntityType.STORAGE_TIER.apiStr());

    // Commodities in actions mapped to their default units.
    // For example, vMem commodity has its default capacity unit as KB.
    private static final ImmutableMap<CommodityDTO.CommodityType, Long> commodityTypeToDefaultUnits =
        new ImmutableMap.Builder<CommodityDTO.CommodityType, Long>()
            .put(CommodityDTO.CommodityType.VMEM, Units.KBYTE)
            .put(CommodityDTO.CommodityType.STORAGE_AMOUNT, Units.MBYTE)
            .put(CommodityDTO.CommodityType.HEAP, Units.KBYTE)
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
                return MessageFormat.format(ACTION_DESCRIPTION_PROVISION,
                    beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(
                        info.getProvision().getEntityToClone().getId()).get()));
            case RESIZE:
                return getResizeActionDescription(entitiesSnapshot, recommendation);
            case ACTIVATE:
                return MessageFormat.format(
                    ACTION_DESCRIPTION_ACTIVATE,
                    beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(
                        info.getActivate().getTarget().getId()).get()));
            case DEACTIVATE:
                return MessageFormat.format(ACTION_DESCRIPTION_DEACTIVATE,
                    beautifyString(ActionDTO.ActionType.SUSPEND.name()),
                    beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(
                        info.getDeactivate().getTarget().getId()).get()));
            case DELETE:
                // TODO need to give savings in terms of cost instead of file size for Cloud entities
                String deleteFilePath = info.getDelete().getFilePath();
                return MessageFormat.format(ACTION_DESCRIPTION_DELETE,
                    deleteFilePath.substring(deleteFilePath.lastIndexOf('/') + 1),
                    beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(
                        info.getDelete().getTarget().getId()).get()),
                    FileUtils.byteCountToDisplaySize(
                        recommendation.getExplanation().getDelete().getSizeKb()
                            * FileUtils.ONE_KB));
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
        if (reconfigure.hasSource()) {
            TopologyEntityDTO currentEntityDTO = entitiesSnapshot.getEntityFromOid(
                reconfigure.getSource().getId()).get();
            return MessageFormat.format(
                ACTION_DESCRIPTION_RECONFIGURE_WITH_SOURCE,
                beautifyEntityTypeAndName(entitiesSnapshot.getEntityFromOid(entityId).get()),
                beautifyCommodityTypes(recommendation.getExplanation().getReconfigure()
                    .getReconfigureCommodityList().stream()
                    .map(ReasonCommodity::getCommodityType)
                    .collect(Collectors.toList())),
                beautifyEntityTypeAndName(currentEntityDTO));
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

        // All moves should have a target entity and a destination.
        TopologyEntityDTO targetEntityDTO = entitiesSnapshot.getEntityFromOid(
            move.getTarget().getId()).get();

        TopologyEntityDTO newEntityDTO = entitiesSnapshot.getEntityFromOid(
            primaryChange.getDestination().getId()).get();

        if (!hasSource) {
            return MessageFormat.format(ACTION_DESCRIPTION_MOVE_WITHOUT_SOURCE,
                beautifyEntityTypeAndName(targetEntityDTO),
                beautifyEntityTypeAndName(newEntityDTO));
        } else {
            TopologyEntityDTO currentEntityDTO = entitiesSnapshot.getEntityFromOid(
                primaryChange.getSource().getId()).get();
            String sourceType = EntityType.forNumber(
                currentEntityDTO.getEntityType()).getDescriptorForType().getFullName();
            String destinationType = EntityType.forNumber(
                newEntityDTO.getEntityType()).getDescriptorForType().getFullName();
            String verb = TIER_VALUES.contains(destinationType)
                && TIER_VALUES.contains(sourceType) ? SCALE : MOVE;
            String resource = "";
            if (primaryChange.hasResource()) {
                Optional<TopologyEntityDTO> resourceEntity = entitiesSnapshot.getEntityFromOid(
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
        TopologyEntityDTO computeTier = entitiesSnapshot.getEntityFromOid(buyRI.getComputeTier().getId()).get();
        final String computeTierName = (computeTier != null) ? computeTier.getDisplayName() : "";

        TopologyEntityDTO masterAccount = entitiesSnapshot.getEntityFromOid(buyRI.getMasterAccount().getId()).get();
        final String masterAccountName = (masterAccount != null) ? masterAccount.getDisplayName() : "";

        TopologyEntityDTO region = entitiesSnapshot.getEntityFromOid(buyRI.getRegionId().getId()).get();
        final String regionName = (region != null) ? region.getDisplayName() : "";

        return MessageFormat.format(ACTION_DESCRIPTION_BUYRI, count, computeTierName,
            masterAccountName, regionName);
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

    /**
     * Convert a list of commodity type numbers to a comma-separated string of readable commodity names.
     *
     * Example: BALLOONING, SWAPPING, CPU_ALLOCATION -> Ballooning, Swapping, Cpu Allocation
     *
     * @param commodityTypes commodity types
     * @return comma-separated string commodity types
     */
    private static String beautifyCommodityTypes(@Nonnull final List<CommodityType> commodityTypes) {
        return commodityTypes.stream()
            .map(commodityType -> ActionDTOUtil.getCommodityDisplayName(commodityType))
            .collect(Collectors.joining(", "));
    }

    /**
     * Returns the entity type and entity name in a nicely formatted way separated by a space.
     * e.g. <p>Virtual Machine vm-test-1</p>
     *
     * @param entityDTO {@link TopologyEntityDTO} entity object.
     * @return The entity type and name separated by a space.
     */
    private static String beautifyEntityTypeAndName(@Nonnull final TopologyEntityDTO entityDTO) {
        return String.format("%s %s",
            beautifyString(EntityType.forNumber(entityDTO.getEntityType()).name()),
            entityDTO.getDisplayName()
        );
    }

    /**
     * Formats the given string by replacing underscores (if they exist) with spaces and returning
     * the new string in "Title Case" format.
     * e.g. VIRTUAL_MACHINE -> Virtual Machine.
     * e.g. SUSPEND -> Suspend.
     *
     * @param str The string that will be formatted.
     * @return The formatted string.
     */
    private static String beautifyString(@Nonnull final String str) {
        return WordUtils.capitalize(str.replace("_"," ").toLowerCase());
    }
}
