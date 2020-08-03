package com.vmturbo.action.orchestrator.action.constraint;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintType;

/**
 * This store has access to all kinds of action constraint store, such as core quota store.
 * This store can be used to update all action constraint stores and
 * return a reference to a specific store.
 */
public class ActionConstraintStoreFactory {

    // An action constraint type to action constraint store map,
    // which is useful for updating action constraint stores
    private static final Map<ActionConstraintType, ActionConstraintStore> constraintStores =
        ImmutableMap.of(
            ActionConstraintType.CORE_QUOTA, CoreQuotaStore.getCoreQuotaStore());

    /**
     * Update action constraint stores using action constraint info.
     *
     * @param actionConstraintInfoMap a action constraint type to action constraint info map,
     *                                which is used to update action constraint stores
     */
    public void updateActionConstraintInfo(
            @Nonnull final Map<ActionConstraintType, ActionConstraintInfo> actionConstraintInfoMap) {
        actionConstraintInfoMap.forEach((actionConstraintType, actionConstraintInfo) ->
            constraintStores.get(actionConstraintType).updateActionConstraintInfo(actionConstraintInfo));
    }

    /**
     * Get the core quota store.
     *
     * @return the core quota store
     */
    public CoreQuotaStore getCoreQuotaStore() {
        return CoreQuotaStore.getCoreQuotaStore();
    }
}
