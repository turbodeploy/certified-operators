package com.vmturbo.topology.processor.historical;

/**
 * This class keeps information for the historical used and peak values of a
 * commodity sold or bought by a service entity.
 */
public class HistoricalCommodityInfo {

    // The type of the commodity
    private int commodityTypeId;

    // The historical used value
    private float historicalUsed;

    // The historical peak value
    private float historicalPeak;

    // The source of the commodity, might be the provider id or the volume id
    private long sourceId;

    // A flag indicating if this commodity is matched to a commodity of the previous cycle
    private boolean matched;

    // A flag indicating if this commodity is existing in the present cycle
    private boolean existing;

    public HistoricalCommodityInfo() {
    }

    public int getCommodityTypeId() {
        return commodityTypeId;
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

    public boolean getExisting() {
        return existing;
    }

    public void setCommodityTypeId(int commodityTypeId) {
        this.commodityTypeId = commodityTypeId;
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

    public void setExisting(boolean existing) {
        this.existing = existing;
    }
}

