package com.vmturbo.repository.service;

import java.util.List;

/**
 * Data type to hold information pertaining to a sold commodity that is relevant to stats
 * retrieval.
 */
class AbridgedSoldCommoditiesForProvider {

    private final int providerType;
    private final List<AbridgedSoldCommodity> soldCommodityList;

    int getProviderType() {
        return providerType;
    }

    List<AbridgedSoldCommodity> getSoldCommodityList() {
        return soldCommodityList;
    }

    AbridgedSoldCommoditiesForProvider(final int providerType,
                                       final List<AbridgedSoldCommodity> soldCommodityList) {
        this.providerType = providerType;
        this.soldCommodityList = soldCommodityList;
    }

    /**
     * An abridged representation of sold commodity. It contains fields only relevant to stats
     * creation.
     */
    static class AbridgedSoldCommodity {
        private final int commodityType;
        private final double capacity;

        AbridgedSoldCommodity(final int commodityType, final double capacity) {
            this.commodityType = commodityType;
            this.capacity = capacity;
        }

        int getCommodityType() {
            return commodityType;
        }

        double getCapacity() {
            return capacity;
        }
    }
}