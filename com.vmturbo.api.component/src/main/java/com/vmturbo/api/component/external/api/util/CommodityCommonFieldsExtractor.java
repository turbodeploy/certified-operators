package com.vmturbo.api.component.external.api.util;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.MessageOrBuilder;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Utility class to extract the common fields of {@link CommodityBoughtDTO} or
 * {@link CommoditySoldDTO}.
 */
public class CommodityCommonFieldsExtractor {

    /**
     * Extract commodity type from commodity bought or sold.
     *
     * @param commodityBoughtOrSold the commodity bought or sold whose fields we want to extract.
     * @return the commodity type.
     */
    @Nonnull
    public static CommodityType getCommodityType(
        @Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        if (commodityBoughtOrSold instanceof CommodityBoughtDTO) {
            return ((CommodityBoughtDTO) commodityBoughtOrSold).getCommodityType();
        }
        else if (commodityBoughtOrSold instanceof CommoditySoldDTO) {
            return ((CommoditySoldDTO) commodityBoughtOrSold).getCommodityType();
        }
        else return CommodityType.getDefaultInstance();
    }

    /**
     * Extract commodity used value from commodity bought or sold.
     *
     * @param commodityBoughtOrSold the commodity bought or sold whose fields we want to extract.
     * @return the commodity used value.
     */
    public static double getUsed(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        if (commodityBoughtOrSold instanceof CommodityBoughtDTO) {
            return ((CommodityBoughtDTO) commodityBoughtOrSold).getUsed();
        }
        else if (commodityBoughtOrSold instanceof CommoditySoldDTO) {
            return ((CommoditySoldDTO) commodityBoughtOrSold).getUsed();
        }
        else return 0d;
    }

    /**
     * Extract commodity peak value from commodity bought or sold.
     *
     * @param commodityBoughtOrSold the commodity bought or sold whose fields we want to extract.
     * @return the commodity peak value.
     */
    public static double getPeak(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        if (commodityBoughtOrSold instanceof CommodityBoughtDTO) {
            return ((CommodityBoughtDTO) commodityBoughtOrSold).getPeak();
        }
        else if (commodityBoughtOrSold instanceof CommoditySoldDTO) {
            return ((CommoditySoldDTO) commodityBoughtOrSold).getPeak();
        }
        else return 0d;
    }

    /**
     * Extract commodity capacity value from commodity sold.
     *
     * @param commodityBoughtOrSold the commodity bought or sold whose fields we want to extract.
     * @return the commodity capacity value.
     */
    public static double getCapacity(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        if (commodityBoughtOrSold instanceof CommoditySoldDTO) {
            return ((CommoditySoldDTO) commodityBoughtOrSold).getCapacity();
        }
        else return 0d;
    }

    /**
     * Extract commodity display name from commodity bought or sold.
     *
     * @param commodityBoughtOrSold the commodity bought or sold whose fields we want to extract.
     * @return the commodity display name.
     */
    @Nullable
    public static String getDisplayName(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        if (commodityBoughtOrSold instanceof CommodityBoughtDTO) {
            return ((CommodityBoughtDTO) commodityBoughtOrSold).getDisplayName();
        }
        else if (commodityBoughtOrSold instanceof CommoditySoldDTO) {
            return ((CommoditySoldDTO) commodityBoughtOrSold).getDisplayName();
        }
        else return null;
    }

    /**
     * Extract aggregate commodity keys from commodity bought or sold.
     *
     * @param commodityBoughtOrSold the commodity bought or sold whose fields we want to extract.
     * @return the aggregate commodity keys.
     */
    @Nonnull
    public static List<String> getAggregates(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        if (commodityBoughtOrSold instanceof CommodityBoughtDTO) {
            return ((CommodityBoughtDTO) commodityBoughtOrSold).getAggregatesList();
        }
        else if (commodityBoughtOrSold instanceof CommoditySoldDTO) {
            return ((CommoditySoldDTO) commodityBoughtOrSold).getAggregatesList();
        }
        else return Collections.emptyList();
    }

    /**
     * Check if the commodity is commodity sold or not.
     *
     * @param commodityBoughtOrSold the commodity bought or sold whose fields we want to extract.
     * @return true if the commodity is commodity sold.
     */
    public static boolean isCommoditySold(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        return commodityBoughtOrSold instanceof CommoditySoldDTO;
    }

    /**
     * Check if the commodity is commodity bought or not.
     *
     * @param commodityBoughtOrSold the commodity bought or sold whose fields we want to extract.
     * @return true if the commodity is commodity bought.
     */
    public static boolean isCommodityBought(@Nonnull final MessageOrBuilder commodityBoughtOrSold) {
        return commodityBoughtOrSold instanceof CommodityBoughtDTO;
    }
}
