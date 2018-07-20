package com.vmturbo.topology.processor.controllable;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ControllableRecordNotFoundException;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.NotSupportedActionStateException;

/**
 * Data access object for creating, updating, deleting controllable tables. And there are three status
 * for controllable record: 'queued', 'in progress', 'succeed'. 1: when topology processor receive
 * action execution request, it will insert records into controllable tables, and its status is 'queued'.
 * 2: when topology processor received action "in progress" notification from probes, it will update
 * related controllable records status to 'in progress'. 3: when topology processor received action succeed
 * notification from probes, it will update related controllable records status to 'succeed'. 4: if topology
 * processor received action failed notification from probes, it will deleted related controllable
 * records.
 */
public interface EntityActionDao {

    /**
     * Insert a list of new records to controllable table, and their status are 'queued' by default.
     * For each of entity id of input entity set, it will create a new record.
     *
     * @param actionId id of action.
     * @param entityIds a set of entity ids.
     */
    void insertAction(final long actionId, @Nonnull final Set<Long> entityIds);

    /**
     * For controllable records with the same input actionId, update their status to new action state
     * and also update their time to current UTC time.
     *
     * @param actionId id of action.
     * @param newState new state of action.
     * @throws ControllableRecordNotFoundException if there is no records with actionId.
     */
    void updateActionState(final long actionId, @Nonnull final ActionState newState)
            throws ControllableRecordNotFoundException;

    /**
     * First it will delete all expired controllable records, for different status records, it may has different expired
     * interval time. Then it will return all non-controllable entities.
     *
     * @return a set of entity ids which means all those entities are not in controllable state.
     */
    Set<Long> getNonControllableEntityIds();
}
