package com.vmturbo.common.protobuf;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility functions for dealing with {@link ActionDTO} protobuf objects.
 */
public class ActionDTOUtil {
    private static final Logger logger = LogManager.getLogger();

    public static final double NORMAL_SEVERITY_THRESHOLD = Integer.getInteger("importance.normal", -1000).doubleValue();
    public static final double MINOR_SEVERITY_THRESHOLD = Integer.getInteger("importance.minor", 0).doubleValue();
    public static final double MAJOR_SEVERITY_THRESHOLD = Integer.getInteger("importance.major", 200).doubleValue();

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
     * The equivalent of {@link ActionDTOUtil#getInvolvedEntities(Action)} for
     * a collection of actions. Returns the union of involved entities for every
     * action in the collection.
     *
     * @param actions The actions to consider.
     * @return A set of IDs of involved entities.
     * @throws UnsupportedActionException If the type of the action is not supported.
     */
    @Nonnull
    public static Set<Long> getInvolvedEntities(@Nonnull final Collection<Action> actions)
            throws UnsupportedActionException {
        final Set<Long> involvedEntitiesSet = new HashSet<>();
        for (final Action action : actions) {
            involvedEntitiesSet.addAll(ActionDTOUtil.getInvolvedEntities(action));
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
    public static Set<Long> getInvolvedEntities(@Nonnull final ActionDTO.Action action)
            throws UnsupportedActionException {
        switch (action.getInfo().getActionTypeCase()) {
            case MOVE:
                final ActionDTO.Move move = action.getInfo().getMove();
                Set<Long> involvedEntities = Sets.newHashSet(move.getTarget().getId());
                for (ChangeProvider change : move.getChangesList()) {
                    long sourceId = change.getSource().getId();
                    if (sourceId != 0) {
                        involvedEntities.add(sourceId);
                    }
                    involvedEntities.add(change.getDestination().getId());
                }
                return ImmutableSet.copyOf(involvedEntities);
            case RESIZE:
                return ImmutableSet.of(action.getInfo().getResize().getTarget().getId());
            case ACTIVATE:
                return ImmutableSet.of(action.getInfo().getActivate().getTarget().getId());
            case DEACTIVATE:
                return ImmutableSet.of(action.getInfo().getDeactivate().getTarget().getId());
            case PROVISION:
                return ImmutableSet.of(action.getInfo().getProvision().getEntityToClone().getId());
            case RECONFIGURE:
                final ActionDTO.Reconfigure reconfigure = action.getInfo().getReconfigure();
                if (reconfigure.hasSource()) {
                    return ImmutableSet.of(reconfigure.getTarget().getId(), reconfigure.getSource().getId());
                } else {
                    return ImmutableSet.of(reconfigure.getTarget().getId());
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
                        && moveExplanation.getChangeProviderExplanation(0).hasInitialPlacement()) {
                        return ActionType.START;
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
     * Get a set of provider entity ids for the input Move action.
     *
     * @param moveAction a {@link Move} action.
     * @return a set of provider entity ids.
     */
    public static Set<Long> getProviderEntityIdsFromMoveAction(@Nonnull final Move moveAction) {
        // right now, only support controllable flag for VM move actions.
        if (moveAction.getTarget().hasType()
                && moveAction.getTarget().getType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            logger.warn("Ignore controllable logic for Move action with type: " +
                    moveAction.getTarget().getType());
            return Collections.emptySet();
        }
        final Set<Long> entityIds = new HashSet<>();
        for (ChangeProvider changeProvider : moveAction.getChangesList()) {
            if (changeProvider.hasSource()) {
                entityIds.add(changeProvider.getSource().getId());
            }
            if (changeProvider.hasDestination()) {
                entityIds.add(changeProvider.getDestination().getId());
            }
        }
        return entityIds;
    }
}
