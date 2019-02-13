package com.vmturbo.common.protobuf.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility functions for dealing with {@link ActionDTO} protobuf objects.
 */
public class ActionDTOUtil {
    private static final Logger logger = LogManager.getLogger();

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

    private ActionDTOUtil() {}

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
     * @throws UnsupportedActionException If the type of the action is not supported.
     */
    @Nonnull
    public static Set<Long> getInvolvedEntityIds(@Nonnull final Collection<Action> actions)
            throws UnsupportedActionException {
        final Set<Long> involvedEntitiesSet = new HashSet<>();
        for (final Action action : actions) {
            involvedEntitiesSet.addAll(ActionDTOUtil.getInvolvedEntityIds(action));
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
            default:
                throw new UnsupportedActionException(action);
        }
    }

    /**
     * Map the importance value to a severity category.
     *
     * @param importance The importance value.
     * @throws IllegalArgumentException When the importance value is a transcendental.
     * @return The name of the severity category.
     */
    @Nonnull
    public static Severity mapImportanceToSeverity(final double importance) {
        if (importance < NORMAL_SEVERITY_THRESHOLD) {
            return Severity.NORMAL;
        } else if (importance < MINOR_SEVERITY_THRESHOLD) {
            return Severity.MINOR;
        } else if (importance < MAJOR_SEVERITY_THRESHOLD) {
            return Severity.MAJOR;
        } else if (importance >= MAJOR_SEVERITY_THRESHOLD) {
            return Severity.CRITICAL;
        }

        throw new IllegalArgumentException(
            "The importance to severity algorithm does not support: " + importance);
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
                // if Move has initial placement explanation, it should be START.
                // When a change within a move has initial placement, all changes will.
                if (explanation.hasMove()) {
                    MoveExplanation moveExplanation = explanation.getMove();
                    if (moveExplanation.getChangeProviderExplanationCount() > 0
                            && moveExplanation.getChangeProviderExplanationList().stream()
                            .anyMatch(m -> m.hasInitialPlacement())) {
                        return ActionType.START;
                    }
                    // If the source and destination is one of the cloud tiers, then the
                    // action type is considered a resize instead of a move.
                    if (action.getInfo().getMove().getChangesList().stream().anyMatch(
                            m -> m.hasSource()
                                    && TopologyDTOUtil.isTierEntityType(m.getDestination().getType())
                                    && TopologyDTOUtil.isTierEntityType(m.getSource().getType()))) {
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
            default:
                return ActionType.NONE;
        }
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
        CommodityDTO.CommodityType commodity = CommodityDTO.CommodityType.forNumber(
                commType.getType());

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
        if (commodity == CommodityDTO.CommodityType.NETWORK) {
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
