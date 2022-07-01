package com.vmturbo.market.runner.wastedfiles;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongMaps;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.commons.Units;

/**
 * Object representing the results of the wasted file analysis.
 */
public class WastedFilesResults {

    /**
     * Singleton instance to represent no results.
     */
    public static final WastedFilesResults EMPTY = new WastedFilesResults(Collections.emptyList(),
            Long2LongMaps.EMPTY_MAP);

    private final Collection<Action> actions;
    private final Long2LongMap storageToStorageAmountReleasedMap;

    WastedFilesResults(List<Action> actions, Long2LongMap storageToStorageAmountReleasedMap) {
        this.actions = actions;
        this.storageToStorageAmountReleasedMap = storageToStorageAmountReleasedMap;
    }

    public Collection<Action> getActions() {
        return actions;
    }

    /**
     * If the input id refers to a provider (e.g. storage) affected by the wasted files actions,
     * return
     * the MBs that will be freed up if the actions are taken (i.e. the wasted MB on the provider).
     *
     * @param oid The provider OID.
     * @return Optional containing the wasted MBs on the provider, or empty optional if provider
     *         is unaffected.
     */
    public OptionalLong getMbReleasedOnProvider(final long oid) {
        if (storageToStorageAmountReleasedMap.containsKey(oid)) {
            return OptionalLong.of(
                    storageToStorageAmountReleasedMap.get(oid) / Units.NUM_OF_KB_IN_MB);
        } else {
            return OptionalLong.empty();
        }
    }
}
