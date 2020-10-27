package com.vmturbo.topology.processor.controllable;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.topology.processor.controllable.EntityActionDaoImp.ActionRecordNotFoundException;

/**
 * Data access object for creating, updating, deleting MOVE or ACTIVATE actions record tables.
 * There are three status for each action record: 'queued', 'in progress', 'succeed'.
 * 1: when topology processor receives an action execution request, it will insert records into tables,
 * and its status is 'queued'. And 'queued' status will also be considered as non-controllable.
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
     * <p>Note that implementations of this method are allowed to (and currently do) collapse
     * different action types into one. I.e. using a different argument for <b>actionType</b>
     * doesn't guarantee a different value will be written to the database. e.g. MOVE_TOGETHER and
     * MOVE are collapsed into a single move type.</p>
     *
     * @param actionId id of action.
     * @param actionType action type
     * @param entityIds a set of entity ids.
     */
    void insertAction(long actionId,
                      @Nonnull ActionItemDTO.ActionType actionType,
                      @Nonnull Set<Long> entityIds)
            throws IllegalArgumentException;

    /**
     * For action records with the same input actionId, update their status to new action state
     * and also update their time to current UTC time.
     *
     * @param actionId id of action.
     * @param newState new state of action.
     * @throws ActionRecordNotFoundException if there is no records with actionId.
     */
    void updateActionState(long actionId, @Nonnull ActionState newState)
            throws ActionRecordNotFoundException;

    /**
     * Deletes all action records effecting the controllable attribute and pertaining to a
     * particular entity OID from the entity_action table.
     *
     * @param entityOid The unique OID of the topology entity for which to remove the records.
     */
    void deleteMoveActions(long entityOid);

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


    /**
     * Check in the action tables to get a set of entities that has scale actions records in the table.
     * @return entity ids not eligible for scale (Currently these entities include VMs (and its providers)
     * on cloud for which 'SCALE' action was executed recently)
     * TODO : Remove providers of such VMs and keep VMs only.
     */
    Set<Long> ineligibleForScaleEntityIds();

    /**
     * Check in the action tables to get a set of entities that has resize actions records in the table.
     * @return entity ids not eligible for resize down (Currently these entities include VMs on premise
     * for which 'RIGHT_SIZE' action was executed recently)
     */
    Set<Long> ineligibleForResizeDownEntityIds();
}
