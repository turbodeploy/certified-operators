package com.vmturbo.platform.analysis.utilities;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;

/**
 * Support Class to get the Cost by CostTableKey.
 * Given a list of CostTuple, it will build a Table where the keys are
 * Region Id, AccountId, License Type.
 * It will track also the cheapest cost for certain attributes.
 * e.g. regionId.
 * In this case to get the cheapest cost use NO_VALUE as input.
 * e.g. getTuple(NO_VALUE, accountId, licenseType)
 */
public class CostTable {
    final static public int NO_VALUE = -1;

    private Map<CostTableKey, CostTuple> map = Maps.newHashMap();
    private Set<Long> accountIds = Sets.newHashSet();

    /**
     * Create a CostTable from a list of CostTuples, tracking also the cheapest Region.
     *
     * @param costTupleList
     */
    public CostTable(final List<CostTuple> costTupleList) {
        for (CostTuple costTuple : costTupleList) {
            CostTableKey key = new CostTableKey(
                    costTuple.hasZoneId() ? costTuple.getZoneId() :
                    costTuple.getRegionId(),
                    costTuple.getBusinessAccountId(),
                    costTuple.getLicenseCommodityType());
            map.put(key, costTuple);

            // track cheapest Region using -1 as region id
            updateCheapest(createCheapestRegionKey(
                    costTuple.getBusinessAccountId(),
                    costTuple.getLicenseCommodityType()),
                    costTuple);

            accountIds.add(costTuple.getBusinessAccountId());
        }
    }

    /**
     * Given a Region, an Account and a License Type, it will return the Tuple with the cost.
     *
     * @param regionId The region Id
     * @param businessAccountId The business account id
     * @param licenseCommodityType The license commodity type
     * @return the CostTuple
     */
    @Nullable
    public CostTuple getTuple(long regionId,
                              long businessAccountId,
                              int licenseCommodityType) {
        return map.get(new CostTableKey(
                regionId,
                businessAccountId,
                licenseCommodityType));
    }

    /**
     * Check if the given Account is present in the Table.
     *
     * @param businessAccountId
     * @return
     */
    public boolean hasAccountId(long businessAccountId) {
        return accountIds.contains(businessAccountId);
    }

    public Set<Long> getAccountIds() {
       return accountIds;
    }

    private void updateCheapest(CostTableKey key,
                                CostTuple costTuple) {
        CostTuple cheapestTuple = map.getOrDefault(key, costTuple);
        if (costTuple.getPrice() <= cheapestTuple.getPrice()) {
            map.put(key, costTuple);
        }
    }

    private CostTableKey createCheapestRegionKey(long businessAccountId,
                                                 int licenseCommodityType) {
        return new CostTableKey(
                NO_VALUE,
                businessAccountId,
                licenseCommodityType);
    }

    /**
     * Class to support the Cost values by Cloud info.
     */
    private static class CostTableKey {
        private long locationId;           // -1 if not provided
        private long businessAccountId; // -1 if not provided
        private int licenseCommodityType;

        CostTableKey(long locationId, long businessAccountId, int licenseCommodityType) {
            this.locationId = locationId;
            this.businessAccountId = businessAccountId;
            this.licenseCommodityType = licenseCommodityType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null)
                return false;
            if (getClass() != o.getClass())
                return false;
            CostTableKey key = (CostTableKey) o;
            return locationId == key.locationId
                    && businessAccountId == key.businessAccountId
                    && licenseCommodityType == key.licenseCommodityType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Long.hashCode(locationId);
            result = prime * result + Long.hashCode(businessAccountId);
            result = prime * result + licenseCommodityType;
            return result;
        }
    }
}
