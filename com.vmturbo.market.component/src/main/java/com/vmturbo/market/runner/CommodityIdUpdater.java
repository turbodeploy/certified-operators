package com.vmturbo.market.runner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.market.topology.conversions.CommodityTypeAllocator;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.utilities.exceptions.ActionCantReplayException;

/**
 * This class is used to update commodity id and create new replay actions.
 */
class CommodityIdUpdater {

    private static final Logger logger = LogManager.getLogger();

    // Commodity id to commodity type mapping
    private Int2ObjectOpenHashMap<CommodityType> commodityIdToCommodityType = new Int2ObjectOpenHashMap<>();

    /**
     * Save commodity id to commodity type mapping, which will be used in the next market analysis
     * to create new replay actions with correct commodity id.
     *
     * @param commodityTypeAllocator commodityTypeAllocator
     * @param replayActions replayActions
     */
    void saveCommodityIdToCommodityType(final CommodityTypeAllocator commodityTypeAllocator,
                                        final ReplayActions replayActions) {
        commodityIdToCommodityType.clear();
        replayActions.getAllActions().forEach(action -> {
            // We can benefit from IntOpenHashSet if action.extractCommodityId() returns such a set because
            // we don't need to convert between primitive int and Integer.
            for (int commodityId : action.extractCommodityIds()) {
                if (commodityIdToCommodityType.containsKey(commodityId)) {
                    continue;
                }
                final CommodityType commodityType =
                    commodityTypeAllocator.marketCommIdToCommodityType(commodityId);
                if (commodityType != null) {
                    commodityIdToCommodityType.put(commodityId, commodityType);
                }
            }
        });
    }

    /**
     * Create new replay actions with correct commodity id.
     *
     * @param analysis analysis
     * @param commodityTypeAllocator commodityTypeAllocator
     */
    void updateReplayActions(final Analysis analysis, final CommodityTypeAllocator commodityTypeAllocator) {
        final ReplayActions replayActions = analysis.getReplayActions();
        if (replayActions.isEmpty()) {
            return;
        }

        // Old commodity id to new commodity id mapping
        final Int2IntOpenHashMap commodityIdMap = new Int2IntOpenHashMap();
        // Set default return value.
        commodityIdMap.defaultReturnValue(NumericIDAllocator.nonAllocableId);
        for (Int2ObjectMap.Entry<CommodityType> entry : commodityIdToCommodityType.int2ObjectEntrySet()) {
            final int commodityId = entry.getIntKey();
            final CommodityType commodityType = entry.getValue();
            if (commodityTypeAllocator.containsCommodityType(commodityType)) {
                commodityIdMap.put(commodityId, commodityTypeAllocator.topologyToMarketCommodityId(commodityType));
            }
        }

        // Create new replay actions
        final List<Action> newActions = new ArrayList<>(replayActions.getActions().size());
        for (Action action : replayActions.getActions()) {
            try {
                newActions.add(action.createActionWithNewCommodityId(commodityIdMap));
            } catch (ActionCantReplayException e) {
                logger.warn("Can't replay action {} because commodity type {} is missing.",
                    action, commodityIdToCommodityType.get(e.getMissingCommodityId()));
            }
        }

        final List<Deactivate> newDeactivateActions = new ArrayList<>(replayActions.getDeactivateActions().size());
        for (Deactivate action : replayActions.getDeactivateActions()) {
            try {
                newDeactivateActions.add(action.createActionWithNewCommodityId(commodityIdMap));
            } catch (ActionCantReplayException e) {
                logger.warn("Can't replay action {} because commodity type {} is missing.",
                    action, commodityIdToCommodityType.get(e.getMissingCommodityId()));
            }
        }

        analysis.setReplayActions(new ReplayActions(newActions, newDeactivateActions));
    }

    /**
     * Return commodityIdToCommodityType. Only used in testing.
     *
     * @return commodityIdToCommodityType
     */
    @VisibleForTesting
    Map<Integer, CommodityType> getCommodityIdToCommodityType() {
        return commodityIdToCommodityType;
    }
}
