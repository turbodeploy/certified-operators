package com.vmturbo.platform.analysis.utilities;

/**
 * A class represents the price information of a commodity.
 * NOTE: the PriceData comparator is overridden to make sure upperBound decides the order
 * @author weiduan
 *
 */
@SuppressWarnings("rawtypes")
public class PriceData implements Comparable {
    private final double upperBound_;
    private final double price_;
    private final boolean isUnitPrice_;
    private final boolean isAccumulative_;
    private final boolean isAppliedToHistoricalUsed_;
    private final long regionId_;

    /**
     * Constructor.
     *  @param upperBound     he upper bound limit of commodity.
     * @param price          price of commodity.
     * @param isUnitPrice    boolean to represent if unitPrice.
     * @param isAccumulative boolean to represent if accumulative pricing.
     * @param isAppliedToHistoricalUsed boolean to define if price is applied to Historical Used.
     * @param regionId       region id of price.
     */
    public PriceData(double upperBound, double price, boolean isUnitPrice,
                     boolean isAccumulative, boolean isAppliedToHistoricalUsed, long regionId) {
        upperBound_ = upperBound;
        price_ = price;
        isUnitPrice_ = isUnitPrice;
        isAccumulative_ = isAccumulative;
        this.isAppliedToHistoricalUsed_ = isAppliedToHistoricalUsed;
        regionId_ = regionId;
    }

    /**
     * Returns the upper bound limit of commodity.
     * @return double.
     */
    public double getUpperBound() {
        return upperBound_;
    }

    /**
     * Returns the price of commodity.
     * @return double.
     */
    public double getPrice() {
        return price_;
    }

    /**
     * Returns true if the price is a unit price.
     * @return boolean.
     */
    public boolean isUnitPrice() {
        return isUnitPrice_;
    }

    /**
     * Returns true if the cost should be accumulated.
     * @return boolean.
     */
    public boolean isAccumulative() {
        return isAccumulative_;
    }

    /**
     * Checks if price is applied to Historical Used value (rather than regular Used).
     *
     * @return True if price is applied to Historical Used value.
     */
    public boolean isAppliedToHistoricalUsed() {
        return isAppliedToHistoricalUsed_;
    }

    /**
     * Getter for the region id.
     *
     * @return the region id
     */
    public long getRegionId() {
        return  regionId_;
    }

    @Override
    public int compareTo(Object other) {
        return Double.compare(upperBound_, ((PriceData)other).getUpperBound());
    }
}
