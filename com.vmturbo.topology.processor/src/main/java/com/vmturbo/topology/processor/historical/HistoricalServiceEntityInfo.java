package com.vmturbo.topology.processor.historical;

import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.Lists;

/**
 * This class keeps information for the historical used and peak values of all the commodities
 * sold and bought by a specific service entity.
 */
public class HistoricalServiceEntityInfo {

    private long seOid;

    private List<HistoricalCommodityInfo> historicalCommoditySold;

    private List<HistoricalCommodityInfo> historicalCommodityBought;


    public HistoricalServiceEntityInfo() {
        historicalCommoditySold = new ArrayList<>();
        historicalCommodityBought = new ArrayList<>();
    }

    public long getSeOid() {
        return seOid;
    }

    public List<HistoricalCommodityInfo> getHistoricalCommoditySold() {
        return historicalCommoditySold;
    }

    public List<HistoricalCommodityInfo> getHistoricalCommodityBought() {
        return historicalCommodityBought;
    }


    public void setSeOid(long seOid) {
        this.seOid = seOid;
    }

    public void setHistoricalCommoditySold(List<HistoricalCommodityInfo> historicalCommoditySold) {
        this.historicalCommoditySold = historicalCommoditySold;
    }

    public void setHistoricalCommodityBought(List<HistoricalCommodityInfo> historicalCommodityBought) {
        this.historicalCommodityBought = historicalCommodityBought;
    }


    public boolean addHistoricalCommoditySold(HistoricalCommodityInfo histCommSold) {
        return historicalCommoditySold.add(histCommSold);
    }

    public boolean addHistoricalCommodityBought(HistoricalCommodityInfo histCommBought) {
        return historicalCommodityBought.add(histCommBought);
    }
}
