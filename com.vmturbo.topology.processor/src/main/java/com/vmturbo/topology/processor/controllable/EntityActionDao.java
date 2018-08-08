package com.vmturbo.topology.processor.controllable;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ControllableRecordNotFoundException;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.NotSupportedActionStateException;

/**
 * Data access object for creating, updating, deleting MOVE or ACTIVATE actions record tables.
 * There are three status for each action record: 'queued', 'in progress', 'succeed'.
 * 1: when topology processor receives an action execution request, it will insert records into tables,
 * and its status is 'queued'.
 * 2: when topology processor receives an action "in progress" notification from probes, it will update
 * related action record status to 'in progress'.
 * 3: when topology processor receives an action succeed notification from probes, it will update related
 * action records status to 'succeed'.
 * 4: if topology processor receives an action failed notification from probes, it will deleted related
 * action records.
 */
public interface EntityActionDao {

    /**
     * Insert a list of new records to action tables, and their status are 'queued' by default.
     * For each of entity id of input entity set, it will create a new record.
     *
     * @param actionId id of action.
     * @param actionType action type
     * @param entityIds a set of entity ids.
     */
    void insertAction(final long actionId, final ActionType actionType, @Nonnull final Set<Long> entityIds)
            throws IllegalArgumentException;

    /**
     * For action records with the same input actionId, update their status to new action state
     * and also update their time to current UTC time.
     *
     * @param actionId id of action.
     * @param newState new state of action.
     * @throws ControllableRecordNotFoundException if there is no records with actionId.
     */
    void updateActionState(final long actionId, @Nonnull final ActionState newState)
            throws ControllableRecordNotFoundException;

    /**
     * First it will delete all expired action records, for different status records, it may has different expired
     * interval time. Then it will return all non-controllable entities.
     *
     * @return a set of entity ids which means all those entities are not in controllable state.
     */
    Set<Long> getNonControllableEntityIds();

    /**
     * Check in the action tables to get a set of entities that has activate actions records in the table.
     * This will return non-suspendable entities
     *
     * @return a set of entity ids which are not suspendable due to activate actions
     */
    Set<Long> getNonSuspendableEntityIds();
}
