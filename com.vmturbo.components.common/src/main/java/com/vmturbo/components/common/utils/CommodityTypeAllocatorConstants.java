package com.vmturbo.components.common.utils;

/**
 * The utility class for CommodityTypeAllocator.
 */
public class CommodityTypeAllocatorConstants {
    /**
     * The baseline count for any commodity with a key. Typically, the total number
     * of non-access commodities in any customer's environment should be ~100.
     * This baseline count will ensure access commodity's CommoditySpecificationTO.Type
     * start with 100000, so that the access commodities would never collide with non-access
     * commodities from previous market cycles.(Important for reservation cache update)
     */
    public static final int ACCESS_COMM_TYPE_START_COUNT = 100000;

    /**
     * Utility class does not have a public or default constructor.
     */
    private CommodityTypeAllocatorConstants() {}

}
