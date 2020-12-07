package com.vmturbo.market.runner.reservedcapacity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMaps;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Data object representing the results of the reserved capacity analysis that are
 * used after the analysis is finished.
 */
public class ReservedCapacityResults {

    /**
     * Empty results instance.
     */
    public static final ReservedCapacityResults EMPTY = new ReservedCapacityResults();

    // Map from a key consists of entityOid and commodityType to new reserved capacity.
    private final Long2ObjectMap<Int2FloatMap> oidCommTypeToReserved =
            new Long2ObjectOpenHashMap<>();

    // A list contains all reservation resize actions.
    private final List<Action> actions = new ArrayList<>();

    /**
     * Get the value that the commodity needs to be resized to by oid and commodityType.
     *
     * @param oid oid of the entity
     * @param commodityType commodity type
     * @return the value that the commodity needs to be resized to
     */
    public float getReservedCapacity(final long oid, final CommodityType commodityType) {
        return oidCommTypeToReserved.getOrDefault(oid, Int2FloatMaps.EMPTY_MAP).getOrDefault(
                commodityType.getType(), 0.0f);
    }

    /**
     * Get all reservation resize actions.
     *
     * @return A Collection contains all reservation resize actions.
     */
    public Collection<Action> getActions() {
        return Collections.unmodifiableCollection(actions);
    }

    void addAction(@Nonnull final Action action) {
        actions.add(action);
    }

    public void putReservedCapacity(long oid, CommodityType commodityType,
            float newReservedCapacity) {
        oidCommTypeToReserved.computeIfAbsent(oid, id -> new Int2FloatOpenHashMap()).put(
                commodityType.getType(), newReservedCapacity);
    }
}
