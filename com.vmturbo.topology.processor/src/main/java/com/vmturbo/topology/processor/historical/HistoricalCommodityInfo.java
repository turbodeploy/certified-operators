package com.vmturbo.topology.processor.historical;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * This class keeps information for the historical used and peak values of a
 * commodity sold or bought by a service entity.
 */
public class HistoricalCommodityInfo {

    // The type and key of the commodity
    private CommodityType commodityTypeAndKey;

    // The historical used value
    private float historicalUsed;

    // The historical peak value
    private float historicalPeak;

    // The source of the commodity, might be the provider id or the volume id
    private long sourceId;

    // A flag indicating if this commodity is matched to a commodity of the previous cycle
    private boolean matched;

    // A flag indicating if this commodity is updated in the present cycle
    private boolean updated;

    public HistoricalCommodityInfo() {
    }

    public CommodityType getCommodityTypeAndKey() {
        return commodityTypeAndKey;
    }

    public float getHistoricalUsed() {
        return historicalUsed;
    }

    public float getHistoricalPeak() {
        return historicalPeak;
    }

    public long getSourceId() {
        return sourceId;
    }

    public boolean getMatched() {
        return matched;
    }

    public boolean getUpdated() {
        return updated;
    }

    public void setCommodityTypeAndKey(CommodityType commodityTypeAndKey) {
        this.commodityTypeAndKey = commodityTypeAndKey;
    }

    public void setHistoricalUsed(float historicalUsed) {
        this.historicalUsed = historicalUsed;
    }

    public void setHistoricalPeak(float historicalPeak) {
        this.historicalPeak = historicalPeak;
    }

    public void setSourceId(long sourceId) {
        this.sourceId = sourceId;
    }

    public void setMatched(boolean matched) {
        this.matched = matched;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }
}

