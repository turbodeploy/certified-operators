package com.vmturbo.platform.analysis.economy;

import java.io.Serializable;

import com.vmturbo.commons.analysis.InvertedIndexBaseTranslator;

/**
 * A mapper that implements methods that are used by the InvertedIndex.
 * This translates commoditiesSold by a Trader, Basket to an array of integers representing CommodityType.
 *
 *
 */
public class InvertedIndexTranslator implements InvertedIndexBaseTranslator<Trader, Basket, Basket>, Serializable {

    /**
     * Create a new inverted index translator.
     */
    public InvertedIndexTranslator() {}

    /**
     * return an array of type values sold by a seller.
     *
     * @param seller whose sold basket is translated to an array of integers
     * @return array of integers representing the types in the basketSold
     */
    public int[] getCommoditySoldTypeArrayFromEntity(final Trader seller) {
        return getCommoditySoldTypeArray(seller.getBasketSold());
    }

    /**
     * return an array of type values present in basket.
     *
     * @param basket whose {@link CommoditySpecification}'s are converted to type integers
     * @return array of integers representing the types in the basket
     */
    public int[] getCommodityBoughtTypeArray(final Basket basket) {
        return basket.stream().mapToInt(CommoditySpecification::getType).toArray();
    }

    /**
     * return an array of type values present in basket.
     *
     * @param basket whose {@link CommoditySpecification}'s are converted to type integers
     * @return array of integers representing the types in the basket
     */
    public int[] getCommoditySoldTypeArray(final Basket basket) {
        return basket.stream().mapToInt(CommoditySpecification::getType).toArray();
    }
}
