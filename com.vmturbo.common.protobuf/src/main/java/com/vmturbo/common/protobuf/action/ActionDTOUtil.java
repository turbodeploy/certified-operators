package com.vmturbo.common.protobuf.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility functions for dealing with {@link ActionDTO} protobuf objects.
 */
public class ActionDTOUtil {
    private static final Logger logger = LogManager.getLogger();

    public static final Set<Integer> NON_DISRUPTIVE_SETTING_COMMODITIES = ImmutableSet.of(
        CommodityDTO.CommodityType.VCPU_VALUE,
        CommodityDTO.CommodityType.VMEM_VALUE);

    public static final double NORMAL_SEVERITY_THRESHOLD = Integer.getInteger("importance.normal", -1000).doubleValue();
    public static final double MINOR_SEVERITY_THRESHOLD = Integer.getInteger("importance.minor", 0).doubleValue();
    public static final double MAJOR_SEVERITY_THRESHOLD = Integer.getInteger("importance.major", 200).doubleValue();

    // string used in the commodity key, in order to separate references to different objs or classes
    // FIXME Embed semantics in the commodity key string is an hack. We need to have a better way to
    // express those relations
    public static final String COMMODITY_KEY_SEPARATOR = "::";

    // this prefix designates that the rest of the string needs to pass through a translation phase
    // This "translation" mechanism is specific to the action explanation and is meant to be resolved
    // in the ActionSpecMapper. We are doing this to save a round of entity service lookups when
    // creating the action explanations, and leaving the entity data to be filled-in when the API
    // remaps the actions to API actions. (since it will be querying the entity service anyways)
    public static final String TRANSLATION_PREFIX = "(^_^)~";

    // the syntax to use for a translatable entry: {entity:oid:field:defaultValue}
    private static final String TRANSLATION_REGEX = "\\{entity:([^:]*?):([^:]*?):([^:]*?)\\}";
    public static final Pattern TRANSLATION_PATTERN = Pattern.compile(TRANSLATION_REGEX);

    public static final Set<Integer> PRIMARY_TIER_VALUES = ImmutableSet.of(
        EntityType.COMPUTE_TIER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE,
        EntityType.DATABASE_TIER_VALUE);

    private ActionDTOUtil() {}

    /**
     * Get the topology context ID of a particular {@link ActionPlanInfo}.
     *
     * @param actionPlan The action plan.
     * @return The topology context ID.
     */
    public static long getActionPlanContextId(@Nonnull final ActionPlanInfo actionPlan) {
        switch (actionPlan.getTypeInfoCase()) {
            case MARKET:
                if (actionPlan.getMarket().getSourceTopologyInfo().hasTopologyContextId()) {
                    return actionPlan.getMarket().getSourceTopologyInfo().getTopologyContextId();
                } else {
                    throw new IllegalArgumentException("Market plan info has no context id: " +
                        actionPlan.getMarket());
                }
            case BUY_RI:
                if (actionPlan.getBuyRi().hasTopologyContextId()) {
                    return actionPlan.getBuyRi().getTopologyContextId();
                } else {
                    throw new IllegalArgumentException("Buy RI plan info has no context id: " +
                        actionPlan.getBuyRi());
                }
            default:
                throw new IllegalArgumentException("Invalid/unset action plan: " +
                    actionPlan.getTypeInfoCase());
        }
    }

    /**
     * Given an action plan, return the {@link ActionPlanType} for the plan. This isn't stored
     * directly in the plan because it's derived from the type of type-specific info the plan
     * contains.
     *
     * @param actionPlanInfo The {@link ActionPlanInfo} for the plan.
     * @return The {@link ActionPlanType} of the action plan.
     */
    @Nonnull
    public static ActionPlanType getActionPlanType(@Nonnull final ActionPlanInfo  actionPlanInfo) {
        switch (actionPlanInfo.getTypeInfoCase()) {
            case MARKET:
                return ActionPlanType.MARKET;
            case BUY_RI:
                return ActionPlanType.BUY_RI;
            default:
                throw new IllegalArgumentException("Invalid/unset action plan: " +
                    actionPlanInfo.getTypeInfoCase());
        }
    }

    /**
     * Get the ID of the entity to the severity of which this action's importance
     * applies. This will be one of the entities involved in the action.
     *
     * @param action The action in question.
     * @return The ID of the entity whose severity is affected by the action.
     * @throws UnsupportedActionException If the type of the action is not supported.
     */
    public static long getSeverityEntity(@Nonnull final ActionDTO.Action action)
            throws UnsupportedActionException {
        final ActionInfo actionInfo = action.getInfo();

        switch (actionInfo.getActionTypeCase()) {
            case MOVE:
                // For move actions, the importance of the action
                // is applied to the source instead of the target,
                // since we're moving the load off of the source.
                List<ChangeProvider> changes = actionInfo.getMove().getChangesList();
                return changes.stream()
                    .filter(provider -> provider.getSource().getType() == EntityType.PHYSICAL_MACHINE_VALUE)
                    .findFirst()
                    // If a PM move exists then use the source ID as the severity entity
                    .map(cp -> cp.getSource().getId())
                    // Otherwise use the source ID of the first provider change
                    .orElse(changes.get(0).getSource().getId());
            case RESIZE:
                return actionInfo.getResize().getTarget().getId();
            case ACTIVATE:
                return actionInfo.getActivate().getTarget().getId();
            case DEACTIVATE:
                return actionInfo.getDeactivate().getTarget().getId();
            case PROVISION:
                // The entity to clone is the target of the action. The
                // newly provisioned entity is the result of the clone.
                return actionInfo.getProvision().getEntityToClone().getId();
            case RECONFIGURE:
                return actionInfo.getReconfigure().getTarget().getId();
            case DELETE:
                return actionInfo.getDelete().getTarget().getId();
            default:
                throw new UnsupportedActionException(action.getId(), actionInfo);
        }
    }

    /**
     * Get the ID of the "main" entity targeted by a specific action.
     * This will be one of the entities involved in the action. It can be thought of as the entity
     * that the action is acting upon.
     *
     * @param action The action in question.
     * @return The ActionEntity of the entity targeted by the action.
     * @throws UnsupportedActionException If the type of the action is not supported.
     */
    public static ActionEntity getPrimaryEntity(@Nonnull final Action action)
            throws UnsupportedActionException {
        final ActionInfo actionInfo = action.getInfo();

        switch (actionInfo.getActionTypeCase()) {
            case MOVE:
                return actionInfo.getMove().getTarget();
            case RECONFIGURE:
                return actionInfo.getReconfigure().getTarget();
            case PROVISION:
                return actionInfo.getProvision().getEntityToClone();
            case RESIZE:
                return actionInfo.getResize().getTarget();
            case ACTIVATE:
                return actionInfo.getActivate().getTarget();
            case DEACTIVATE:
                return actionInfo.getDeactivate().getTarget();
            case DELETE:
                return actionInfo.getDelete().getTarget();
            case BUYRI:
                return actionInfo.getBuyRi().getComputeTier();
            default:
                throw new UnsupportedActionException(action.getId(), actionInfo);
        }
    }

    /**
     * Get the ID of the "main" entity targeted by a specific action.
     * This will be one of the entities involved in the action. It can be thought of as the entity
     * that the action is acting upon.
     *
     * @param action The action in question.
     * @return The ID of the entity targeted by the action.
     * @throws UnsupportedActionException If the type of the action is not supported.
     */
    public static long getPrimaryEntityId(@Nonnull final Action action)
            throws UnsupportedActionException {
        return getPrimaryEntity(action).getId();
    }

    /**
     * The equivalent of {@link ActionDTOUtil#getInvolvedEntities(Action)} for
     * a collection of actions. Returns the union of involved entities for every
     * action in the collection.
     *
     * @param actions The actions to consider.
     * @return A set of IDs of involved entities.
     */
    @Nonnull
    public static Set<Long> getInvolvedEntityIds(@Nonnull final Collection<Action> actions) {
        final Set<Long> involvedEntitiesSet = new HashSet<>();

        // Avoid allocation of actual map until we actually have an error.
        Map<ActionTypeCase, MutableInt> unsupportedActionTypes = Collections.emptyMap();

        for (final Action action : actions) {
            try {
                involvedEntitiesSet.addAll(ActionDTOUtil.getInvolvedEntityIds(action));
            } catch (UnsupportedActionException e) {
                if (unsupportedActionTypes.isEmpty()) {
                    unsupportedActionTypes = new HashMap<>();
                }
                unsupportedActionTypes.computeIfAbsent(e.getActionType(), k -> new MutableInt()).increment();
            }
        }

        if (!unsupportedActionTypes.isEmpty()) {
            logger.error("Encountered unsupported actions of the following types: {}",
                unsupportedActionTypes);
        }

        return involvedEntitiesSet;
    }

    /**
     * Get the entities that are involved in an action.
     * Involved entities are any entities which the action directly
     * affects. For example, in a move the involved entities are the
     * target, source, and destination.
     *
     * @param action The action to consider.
     * @return A set of IDs of involved entities.
     * @throws UnsupportedActionException If the type of the action is not supported.
     */
    @Nonnull
    public static Set<Long> getInvolvedEntityIds(@Nonnull final ActionDTO.Action action)
            throws UnsupportedActionException {
        return getInvolvedEntities(action).stream()
            .map(ActionEntity::getId)
            .collect(Collectors.toSet());
    }

    @Nonnull
    public static List<ActionEntity> getInvolvedEntities(@Nonnull final ActionDTO.Action action)
            throws UnsupportedActionException {
        switch (action.getInfo().getActionTypeCase()) {
            case MOVE:
                final ActionDTO.Move move = action.getInfo().getMove();
                List<ActionEntity> retList = new ArrayList<>();
                retList.add(move.getTarget());
                for (ChangeProvider change : move.getChangesList()) {
                    if (change.getSource().getId() != 0) {
                        retList.add(change.getSource());
                    }
                    if (change.hasResource()) {
                        retList.add(change.getResource());
                    }
                    retList.add(change.getDestination());
                }
                return retList;
            case RESIZE:
                return Collections.singletonList(action.getInfo().getResize().getTarget());
            case ACTIVATE:
                return Collections.singletonList(action.getInfo().getActivate().getTarget());
            case DEACTIVATE:
                return Collections.singletonList(action.getInfo().getDeactivate().getTarget());
            case PROVISION:
                return Collections.singletonList(action.getInfo().getProvision().getEntityToClone());
            case RECONFIGURE:
                final ActionDTO.Reconfigure reconfigure = action.getInfo().getReconfigure();
                if (reconfigure.hasSource()) {
                    return Arrays.asList(reconfigure.getTarget(), reconfigure.getSource());
                } else {
                    return Collections.singletonList(reconfigure.getTarget());
                }
            case DELETE:
                return Collections.singletonList(action.getInfo().getDelete().getTarget());
            case BUYRI:
                final BuyRI buyRi = action.getInfo().getBuyRi();
                List<ActionEntity> actionEntities = new ArrayList<>();
                actionEntities.add(buyRi.getComputeTier());
                actionEntities.add(buyRi.getRegionId());
                actionEntities.add(buyRi.getMasterAccount());
                actionEntities.add(buyRi.getComputeTier());
                return actionEntities;
            default:
                throw new UnsupportedActionException(action);
        }
    }

    /**
     * Map the action category to a severity category.
     *
     * @param category The {@link ActionDTO.ActionCategory}.
     * @return The name of the severity category.
     */
    @Nonnull
    public static Severity mapActionCategoryToSeverity(@Nonnull final ActionCategory category) {
        switch (category) {
            case PERFORMANCE_ASSURANCE:
                return Severity.CRITICAL;
            case COMPLIANCE:
                return Severity.CRITICAL;
            case PREVENTION:
                return Severity.MAJOR;
            case EFFICIENCY_IMPROVEMENT:
                return Severity.MINOR;
            default:
                return Severity.NORMAL;
        }
    }

    /**
     * Set the severity using the naming scheme expected by the UI.
     *
     * @param severity The severity whose name should be retrieved
     * @return the name of the severity with proper Capitalization
     */
    public static String getSeverityName(@Nonnull final Severity severity) {
        return StringUtils.capitalize(severity.name().toLowerCase());
    }

    /**
     * Return the {@link ActionType} that matches the contents of an {@link Action}.
     *
     * @param action an action with explanations
     * @return the type corresponding with the action
     */
    @Nonnull
    public static ActionType getActionInfoActionType(@Nonnull final Action action) {
        switch (action.getInfo().getActionTypeCase()) {
            case MOVE:
                final Explanation explanation = action.getExplanation();
                if (explanation.hasMove()) {
                    if (isMoveActivate(action)) {
                        return ActionType.ACTIVATE;
                    }
                    if (isMoveResize(action)) {
                        return ActionType.RESIZE;
                    }
                }
                return ActionType.MOVE;
            case RECONFIGURE:
                return ActionType.RECONFIGURE;
            case PROVISION:
                return ActionType.PROVISION;
            case RESIZE:
                return ActionType.RESIZE;
            case ACTIVATE:
                return ActionType.ACTIVATE;
            case DEACTIVATE:
                return ActionType.DEACTIVATE;
            case DELETE:
                return ActionType.DELETE;
            case BUYRI:
                return ActionType.BUY_RI;
            default:
                return ActionType.NONE;
        }
    }

    /**
     * Checks if a given MOVE action is actually an ACTIVATE based on the following two cases:
     * <ul>
     *     <li>if Move has initial placement explanation, it should be ACTIVATE. When a change
     *     within a move has initial placement, all changes will.</li>
     *     <li>If the action doesn't have a source, it's ACTIVATE.</li>
     * </ul>
     *
     * @param action The given MOVE {@link Action}
     * @return true if the given MOVE is ACTIVATE, false otherwise.
     */
    private static Boolean isMoveActivate(@Nonnull final Action action) {
        MoveExplanation moveExplanation = action.getExplanation().getMove();
        return (moveExplanation.getChangeProviderExplanationCount() > 0
            && moveExplanation.getChangeProviderExplanationList().stream()
            .anyMatch(m -> m.hasInitialPlacement())) ||
            action.getInfo().getMove().getChangesList().stream().anyMatch(m -> !m.hasSource());
    }

    /**
     * Checks if a given MOVE action is actually a RESIZE based on the following case:
     * <ul>
     *     <li>If the source and destination is one of the cloud tiers, then the action type is
     *     considered a resize instead of a move.</li>
     * </ul>
     *
     * @param action The given MOVE {@link Action}
     * @return true if the given MOVE is a RESIZE, false otherwise.
     */
    private static Boolean isMoveResize(@Nonnull final Action action) {
        return action.getInfo().getMove().getChangesList().stream().anyMatch(
            m -> m.hasSource()
                && TopologyDTOUtil.isTierEntityType(m.getDestination().getType())
                && TopologyDTOUtil.isTierEntityType(m.getSource().getType()));
    }

    /**
     * Get the "primary" {@link ChangeProvider} for a particular move action.
     * Only relevant in compound moves. Generally, if a PM move is accompanied by a storage move,
     * the PM move is considered primary.
     *
     * @param move The {@link ActionDTO.Move} action.
     * @return The primary {@link ChangeProvider}.
     */
    @Nonnull
    public static ChangeProvider getPrimaryChangeProvider(@Nonnull final ActionDTO.Move move) {
        return move.getChanges(getPrimaryChangeProviderIdx(move));
    }

    private static int getPrimaryChangeProviderIdx(@Nonnull final ActionDTO.Move move) {
        for (int i = 0; i < move.getChangesList().size(); ++i) {
            final ChangeProvider change = move.getChanges(i);
            if (isPrimaryEntityType(change.getDestination().getType())) {
                return i;
            }
        }
        return 0;
    }

    /**
     * Is the entity type a primary entity type?
     *
     * Only relevant in compound moves. Generally, if a PM move is accompanied by a storage move,
     * the PM move is considered primary.
     * @param entityType the entity type to check
     * @return true if the entity type is a primary entity type. false otherwise.
     */
    public static boolean isPrimaryEntityType(int entityType) {
        return entityType == EntityType.PHYSICAL_MACHINE_VALUE || PRIMARY_TIER_VALUES.contains(entityType);
    }

    /**
     * Get the reason commodities for a particular {@link ActionDTO.Action}. These are the
     * commodities which, in some way, caused the action to happen. e.g. if a VM needs to
     * be sized up due to a lack of VMem, "VMem" would be the reason commodity.
     *
     * @param action The {@link ActionDTO.Action}.
     * @return A stream of {@link TopologyDTO.CommodityType}s that caused the action. May be an
     *         empty stream. May contain multiple commodities, depending on the action explanation.
     */
    @Nonnull
    public static Stream<ReasonCommodity> getReasonCommodities(@Nonnull final ActionDTO.Action action) {
        final ActionInfo actionInfo = action.getInfo();
        switch (action.getInfo().getActionTypeCase()) {
            case MOVE:
                final ChangeProviderExplanation changeExp = action.getExplanation().getMove()
                    .getChangeProviderExplanation(getPrimaryChangeProviderIdx(actionInfo.getMove()));
                switch (changeExp.getChangeProviderExplanationTypeCase()) {
                    case COMPLIANCE:
                        return changeExp.getCompliance().getMissingCommoditiesList().stream();
                    case CONGESTION:
                        return changeExp.getCongestion().getCongestedCommoditiesList().stream();
                    case EFFICIENCY:
                        return Stream.concat(
                            changeExp.getEfficiency().getCongestedCommoditiesList().stream(),
                            changeExp.getEfficiency().getUnderUtilizedCommoditiesList().stream());
                    case EVACUATION:
                    case INITIALPLACEMENT:
                    case PERFORMANCE:
                    default:
                        return Stream.empty();
                }
            case RECONFIGURE:
                return action.getExplanation().getReconfigure().getReconfigureCommodityList().stream();
            case PROVISION:
                final ProvisionExplanation provisionExplanation = action.getExplanation().getProvision();
                if (provisionExplanation.hasProvisionByDemandExplanation()) {
                    return provisionExplanation.getProvisionByDemandExplanation()
                        .getCommodityMaxAmountAvailableList().stream()
                        .map(CommodityMaxAmountAvailableEntry::getCommodityBaseType)
                        .map(baseType -> createReasonCommodityFromBaseType(baseType));
                } else if (provisionExplanation.hasProvisionBySupplyExplanation()) {
                    return Stream.of(provisionExplanation.getProvisionBySupplyExplanation()
                                    .getMostExpensiveCommodityInfo());
                } else {
                    return Stream.empty();
                }
            case RESIZE:
                return Stream.of(actionInfo.getResize().getCommodityType())
                                .map(ct -> createReasonCommodityFromCommodityType(ct));
            case ACTIVATE:
                return action.getInfo().getActivate().getTriggeringCommoditiesList()
                    .stream().map(ct -> createReasonCommodityFromCommodityType(ct));
            case DEACTIVATE:
                return action.getInfo().getDeactivate().getTriggeringCommoditiesList()
                    .stream().map(ct -> createReasonCommodityFromCommodityType(ct));
            default:
                return Stream.empty();
        }
    }

    @Nonnull
    private static ReasonCommodity createReasonCommodityFromBaseType(int baseType) {
        return createReasonCommodityFromCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(baseType).build());
    }

    @Nonnull
    private static ReasonCommodity
            createReasonCommodityFromCommodityType(@Nonnull TopologyDTO.CommodityType ct) {
        return ReasonCommodity.newBuilder().setCommodityType(ct).build();
    }

    /**
     * Gets the display name of the commodity from the {@link CommodityType}.
     * The display name usually is the pretty print version of the Commodity type.
     * Example: STORAGE_PROVISION -> Storage Provision.
     * There can be commodities that have special cases, like Network, where we want to show more
     * information to the customer (like the network name itself also).
     *
     * @param commType the {@link CommodityType} for which the displayName is to be retrieved
     * @return display name string of the commodity
     */
    public static String getCommodityDisplayName(@Nonnull TopologyDTO.CommodityType commType) {
        UICommodityType commodity = UICommodityType.fromType(commType.getType());

        final String commodityName = commodity.name();

        String commodityDisplayName = getSpaceSeparatedWordsFromCamelCaseString(
                CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, commodityName));

        // If the commodity type is network, we need to append the name of the network.
        // The name of the network is stored in the key currently. So we append the key.
        // TODO: But, this is a hack. We need to come up with a framework for showing display names
        // of commodities.
        // Example:
        // a network commodity with key "Network::testNetwork2" should show in the explanation as
        // Network testNetwork2 and not Network Network::testNetwork2
        if (commodity == UICommodityType.NETWORK) {
            String commKey = commType.getKey();

            // normalize the prefixes to everything lower case
            String commKeyPrefixExpected = commodityName.toLowerCase() + COMMODITY_KEY_SEPARATOR;
            String commKeyPrefix = commKey.substring(0, commKeyPrefixExpected.length()).toLowerCase();
            // and check if the key has the prefix, and remove it.
            if (commKeyPrefix.startsWith(commKeyPrefixExpected)) {
                commKey = commKey.substring(commKeyPrefixExpected.length(), commKey.length());
            }

            // append the modified key to the display name.
            // This will show the specific network name in it.
            commodityDisplayName += " " + commKey;
        }

        return commodityDisplayName;
    }

    /**
     * Convert camel case (e.g. PhysicalMachine) into strings with the same
     * capitalization plus blank spaces (e.g. "Physical Machine"). It also splits numbers,
     * e.g. "May5" -> "May 5" and respects upper case runs, e.g. (PDFLoader -> "PDF Loader").
     *
     * The regex uses zero-length pattern matching with look-behind and look-forward, and is
     * taken from - http://stackoverflow.com/questions/2559759.
     *
     * @param str any string
     * @return see description
     */
    public static String getSpaceSeparatedWordsFromCamelCaseString(@Nonnull final String str) {
        return str.replaceAll(String.format("%s|%s|%s",
                "(?<=[A-Z])(?=[A-Z][a-z])",
                "(?<=[^A-Z])(?=[A-Z])",
                "(?<=[A-Za-z])(?=[^A-Za-z])"),
                " ");
    }

    /**
     * Convert a string from UPPER_UNDERSCORE format to Mixed Spaces format. e.g.:
     *
     *      I_AM_A_CONSTANT ==> I Am A Constant
     *
     * @param input
     * @return
     */
    public static String upperUnderScoreToMixedSpaces(@Nonnull final String input) {
        // replace underscores with spaces
        return WordUtils.capitalizeFully(input.replace("_", " "));
    }

    /**
     * Convert a string from "Mixed Spaces" to "UPPER_UNDERSCORE" format. e.g.:
     *
     *     I Am A Constant ==> I_AM_A_CONSTANT
     *
     * This is the inverse of {@link ActionDTOUtil#upperUnderScoreToMixedSpaces(String)}.
     * @param input
     * @return
     */
    @Nonnull
    public static String mixedSpacesToUpperUnderScore(@Nonnull final String input) {
        return input.replace(" ", "_").toUpperCase();
    }

    /**
     * Create a translation chunk using the given properties.
     *
     * @param entityOid
     * @param field
     * @param defaultValue
     * @return
     */
    public static String createTranslationBlock(long entityOid, @Nonnull String field, @Nonnull String defaultValue) {
        return "{entity:"+ entityOid +":"+ field +":"+ defaultValue +"}";
    }
}
