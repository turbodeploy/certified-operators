package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * This class provides access to the information from the PriceTableStore,  ReservedInstanceBoughtStore
 * and ReservedInstanceSpecStore.
 * Local data structures are populated in the constructor to ensure the data is not stale and to provide easier
 * access to the stored information.
 * An instance of this class must be created for each Reserved Instance buy algorithm.
 */
public class ReservedInstanceAnalyzerRateAndRIs {

    private static final Logger logger = LogManager.getLogger();

    private static final int HOURS_IN_A_MONTH = 730;

    /*
     * Inputs.
     */
    // An interface for obtaining pricing.
    private final PriceTableStore priceTableStore;

    // The Reserved Instance specification store.
    private final ReservedInstanceSpecStore riSpecStore;

    // The inventory of RIs that have already been purchased.
    private final ReservedInstanceBoughtStore riBoughtStore;

    /*
     * Internal data structures populated at construction time to represent the data in
     * PriceTableStore,  ReservedInstanceBoughtStore and ReservedInstanceSpecStore.
     */
    // PriceTableStore: Map from region OID to OnDemandPriceTable
    private Map<Long, OnDemandPriceTable> onDemandRateMap;

    // PriceTableStore: Map from ReservedInstanceSpec OID to ReservedInstancePrice.
    private Map<Long, ReservedInstancePrice> reservedInstanceRateMap;

    // ReservedInstanceSpecStore: Map from ReservedInstanceSpecInfo attributes to ReservedInstanceSpec.
    private Map<ReservedInstanceSpecKey, ReservedInstanceSpec> reservedInstanceSpecKeyMap;

    // ReservedInstanceSpecStore: Map from ReservedInstanceSpec OID to ReservedInstanceSpec.
    Map<Long, ReservedInstanceSpec> reservedInstanceSpecIdMap;

    // ReservedInstanceBoughtStore: Table row: BusinessAccountId column: availabilityZoneId to
    // list of ReservedInstanceBoughtInfo
    private Table<Long, Long, List<ReservedInstanceBoughtInfo>> reservedInstanceBoughtInfoTable;

    /**
     * Constructor.  Build all the internal data structures.
     * @param priceTableStore price table store.
     * @param riSpecStore reserved instance specification store.
     * @param riBoughtStore reserved instance bought store (exiting RIs).
     */
    public ReservedInstanceAnalyzerRateAndRIs(@Nonnull PriceTableStore priceTableStore,
                                              @Nonnull ReservedInstanceSpecStore riSpecStore,
                                              @Nonnull ReservedInstanceBoughtStore riBoughtStore) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.riSpecStore = Objects.requireNonNull(riSpecStore);
        this.riBoughtStore = Objects.requireNonNull(riBoughtStore);

        // compute internal data structures
        populateOnDemandRateMap(priceTableStore);
        populateReservedInstanceSpecIdMap(riSpecStore);
        populateReservedInstanceSpecKeyMap(riSpecStore);
        populateReservedInstanceBoughtInfoTable(riBoughtStore);
        populateReservedInstanceRateMap(priceTableStore);
    }

    /**
     * Use for JUnit testing only.
     */
    @VisibleForTesting
    ReservedInstanceAnalyzerRateAndRIs() {
        this.priceTableStore = null;
        this.riSpecStore = null;
        this.riBoughtStore = null;
    }
    /*
     * Methods for accessing locally stored data.
     */

    /**
     * Given the regional context and RI purchase constraints, return the on-demand and RI rates.
     * @param regionalContext regional context.
     * @param constraints reserved instance purchase constraints.
     * @return on-demand and RI rates for this regional context and purchase constraints.
     */
    public PricingProviderResult findRates(ReservedInstanceRegionalContext regionalContext,
                                           ReservedInstancePurchaseConstraints constraints) {
        return new PricingProviderResult(lookupOnDemandRate(regionalContext),
            lookupReservedInstanceRate(regionalContext, constraints));
    }

    /**
     * Find the on-demand rate for this regional context.
     *
     * @param regionalContext regional context.
     * @return on-demand rate or 0 if not found.
     */
    public float lookupOnDemandRate(@Nonnull ReservedInstanceRegionalContext regionalContext) {
        float onDemandRate = 0f;

        // onDemandPriceTableByRegion from constructor
        if (onDemandRateMap == null) {
            logger.warn("lookupOnDemandRate() on-demand rates are not available!");
            return onDemandRate;
        }
        OnDemandPriceTable onDemandPriceTable = onDemandRateMap.get(regionalContext.getRegionId());
        if (onDemandPriceTable == null) {
            logger.warn("lookupOnDemandRate() could not find on-demand rates for region in context={}",
                regionalContext);
            return onDemandRate;
        }
        Map<Long, ComputeTierPriceList> onDemandMapByTier = onDemandPriceTable.getComputePricesByTierIdMap();
        if (onDemandMapByTier == null) {
            logger.warn("lookupOnDemandRate() could not find on-demand rate by tier in context={}",
                regionalContext);
            return onDemandRate;
        }
        ComputeTierPriceList computeTierRates = onDemandMapByTier.get(regionalContext.getComputeTier().getOid());
        if (computeTierRates == null) {
            logger.warn("getRates() could not find on-demand rate by tier={}({}) in region={}",
                regionalContext.getComputeTier().getDisplayName(), regionalContext.getComputeTier().getOid(),
                regionalContext.getRegionId());
            return onDemandRate;
        }
        ComputeTierConfigPrice baseRate = computeTierRates.getBasePrice();
        if (baseRate == null) {
            logger.warn("lookupOnDemandRate() could not find on-demand base rate for tier={} in region={}",
                regionalContext.getComputeTier().getDisplayName(),
                regionalContext.getRegionId());
            return onDemandRate;
        }
        List<Price> prices = baseRate.getPricesList();
        if (prices.size() > 1) {
            logger.warn("lookupOnDemandRate for masterID={} regionId={} tier={} platform={} tenancy={} prices.size()={} > 1",
                regionalContext.getMasterAccountId(), regionalContext.getRegionId(),
                regionalContext.getComputeTier().getDisplayName(), regionalContext.getPlatform().name(),
                regionalContext.getTenancy().name(), prices.size());
        }
        for (Price price : prices) {
            onDemandRate += computeOnDemandRate(price, regionalContext, "base");
        }
        for (ComputeTierConfigPrice adjustment:  computeTierRates.getPerConfigurationPriceAdjustmentsList()) {
            if (adjustment.getTenancy() == regionalContext.getTenancy() &&
                adjustment.getGuestOsType() == regionalContext.getPlatform()) {
                for (Price price : adjustment.getPricesList()) {
                    onDemandRate += computeOnDemandRate(price, regionalContext, "adjustment");
                }
            }
        }
        logger.info("lookupOnDemandRate() onDemandRate={} for context={}",
            onDemandRate, regionalContext);
        return onDemandRate;
    }

    /**
     * Given a Price, compute the hourly rate.
     *
     * @param price the price from the price table
     * @param context regional context (for logging)
     * @param type base or adjustment for logging
     * @return onDemand rate
     */
    private float computeOnDemandRate(Price price, ReservedInstanceRegionalContext context, String type) {
        float rate = 0;
        Unit unit = price.getUnit();
        CurrencyAmount currencyAmount = price.getPriceAmount();
        if (!currencyAmount.hasAmount()) {
            logger.warn("computeOnDemandRate() type={} currencyAmount={} has no amount for context={}",
                type, type, currencyAmount, context);
            return rate;
        }
        double amount = currencyAmount.getAmount();
        int divisor = 1;
        if (unit == Unit.MONTH) {
            divisor = HOURS_IN_A_MONTH;
        } else if (unit == Unit.TOTAL) {
            logger.warn("computeOnDemandRate() type={} unit == Unit.TOTAL for context={}",
                type, context);
            return rate;
        } else if (unit != Unit.HOURS) {
            logger.warn("computeOnDemandRate() type={} unit != Unit.HOURS for context={}",
                type, context);
            return rate;
        }
         rate = new Double(amount / (float)divisor).floatValue();
        logger.debug("computeOnDemandRate() type={} hourly rate={} divisor={} for context={}",
            type, rate, divisor, context);
        return rate;
    }

    /**
     * Get the on-demand rate information from the PriceTableStore.
     *
     * @param store price table store.  Passed in to ease JUnit testing.
     */
    @VisibleForTesting
    @Nullable
    void populateOnDemandRateMap(PriceTableStore store) {
       PriceTable priceTable = store.getMergedPriceTable();
        if (priceTable == null) {
            logger.warn("populateOnDemandRateMap() priceTableStore.getMergedPriceTable() == null!");
            return;
        }
        onDemandRateMap = priceTable.getOnDemandPriceByRegionIdMap();
        logger.info("populateOnDemandRateMap size={}", onDemandRateMap.size());
    }

    /**
     * Lookup the Reserved Instance rate.
     * The effective hourly price (actual hourly + amortized up-front) cost.
     *
     * @param regionalContext regional context
     * @param constraints purchase constraints
     * @return reserved instance rate or Float.MAX_VALUE if can't be found.
     */
    public float lookupReservedInstanceRate(@Nonnull ReservedInstanceRegionalContext regionalContext,
                                            @Nonnull ReservedInstancePurchaseConstraints constraints) {
        ReservedInstanceSpecKey key = new ReservedInstanceSpecKey(regionalContext, constraints);
        ReservedInstanceSpec spec = reservedInstanceSpecKeyMap.get(key);
        if (spec == null) {
            logger.warn("lookupReservedInstanceRate() can't find ReservedInstanceSpec for key={}",
                key);

            return Float.MAX_VALUE;
        }
        long specId = spec.getId();
        ReservedInstancePrice riPrice = reservedInstanceRateMap.get(specId);
        if (riPrice == null) {
            logger.warn("lookupReservedInstanceRate() can't find rate for ReservedInstanceSpecId={} for key={}",
                specId, key);
            return Float.MAX_VALUE;
        }
        Price upFrontPrice = riPrice.getUpfrontPrice();
        Price hourlyPrice = riPrice.getRecurringPrice();
        CurrencyAmount upFrontCurrencyAmount = upFrontPrice.getPriceAmount();
        CurrencyAmount hourlyCurrencyAmount = hourlyPrice.getPriceAmount();
        double upFrontAmount = upFrontCurrencyAmount.getAmount();
        double hourlyAmount = hourlyCurrencyAmount.getAmount();
        float upFrontAmortizedCost = 0f;
        if (upFrontAmount > 0) {
            upFrontAmortizedCost = new Double(upFrontAmount).floatValue() /
                (float)(constraints.getTermInYears() * 12 * HOURS_IN_A_MONTH);
        }
        float riRate = upFrontAmortizedCost + new Double(hourlyAmount).floatValue();
        logger.info("lookupReservedInstanceRate() riRate={} = hourlyAmount={} + upFrontAmortized{} (upFront={}) for specId={} context={} constraints={}",
            riRate, hourlyAmount, upFrontAmortizedCost, upFrontAmount, specId, regionalContext, constraints);
        return riRate;
    }

    /**
     * Populate the reservedInstanceRateMap with the reserved instance rate information from the
     * PriceTableStore.
     *
     * @param store price table store.  Passed in to ease JUnit testing.
     */
    @Nullable
    @VisibleForTesting
    void populateReservedInstanceRateMap(PriceTableStore store) {
        ReservedInstancePriceTable riPriceTable = store.getMergedRiPriceTable();
        if (riPriceTable == null) {
            logger.warn("populateReservedInstanceRateMap() priceTableStore.getMergedRiPriceTable() == null!");
            return;
        }
        validateRiPricesBySpecIdMap(riPriceTable.getRiPricesBySpecIdMap());
        reservedInstanceRateMap = riPriceTable.getRiPricesBySpecIdMap();
        logger.info("populateReservedInstanceRateMap size={}", reservedInstanceRateMap.size());
    }

    /**
     * Validate the riPriceTable.getRiPricesBySpecIdMap.
     * This method must be called after populateReservedInstanceSpecIdMap() is called.
     * 1) Find all the regions where there are RI rates.
     *
     * @param map map from ReservedInstanceSpecId to ReservedInstancePrice.
     */
    private void validateRiPricesBySpecIdMap(Map<Long, ReservedInstancePrice> map) {
        Map<Long, Integer> regionIds = new HashMap<>();
        for (Long id: map.keySet()) {
            ReservedInstanceSpec spec = reservedInstanceSpecIdMap.get(id.longValue());
            if (spec == null) {
                logger.warn("validateRiPricesBySpecIdMap() RISpecId={} in riPriceTable.getRiPricesBySpecIdMap() not found in reservedInstanceSpecIdMap",
                    id);
                continue;
            }
            ReservedInstanceSpecInfo info = spec.getReservedInstanceSpecInfo();
            if (info == null) {
                logger.error("validateRiPricesBySpecIdMap() RISpec with Id={} does not have info!", id);
                continue;
            }
            long regionId = info.getRegionId();
            Integer count = regionIds.get(regionId);
            if (count == null) {
                regionIds.put(regionId, 1);
            } else {
                regionIds.put(regionId, count + 1);
            }
        }
        StringBuffer buffer = new StringBuffer();
        buffer.append("validateRiPricesBySpecIdMap() number of regions=").append(regionIds.size());
        for (Long regionId: regionIds.keySet()) {
            buffer.append("\n\tregionId=").append(regionId).append(" count=").append(regionIds.get(regionId));
        }
        logger.info(buffer.toString());
    }

    /**
     * Access the reservedInstanceSpecIdMap to look up ReservedInstanceSpec.
     *
     * @param reservedInstanceSpecId the ReservedInstanceSpec ID
     * @return specification of a reserved instance
     */
    @Nullable
    public ReservedInstanceSpec lookupReservedInstanceSpecWithId(long reservedInstanceSpecId) {
        return reservedInstanceSpecIdMap.get(reservedInstanceSpecId);
    }

    /**
     * Populate reservedInstanceSpecIdMap with the reserved instance specs from the ReservedInstanceSpecStore.
     *
     * @param store reserved instance spec store.  Pass in to ease JUnit testing.
     */
    @VisibleForTesting
    void populateReservedInstanceSpecIdMap(ReservedInstanceSpecStore store) {
        reservedInstanceSpecIdMap = store.getAllReservedInstanceSpec()
            .stream()
            .collect(Collectors.toMap(ReservedInstanceSpec::getId,
                Function.identity()));
        logger.info("populateReservedInstanceSpecIdMap() size={}", reservedInstanceSpecIdMap.size());
    }

    /**
     * Access the reservedInstanceBoughtTable to return existing reserved instances for a master
     * account and availability zone.
     *
     * @param masterAccountId master account ID
     * @param zoneId avialability zone ID, constraint must be > 0.
     * @return reserved instance inventory
     */
    @Nullable
    public List<ReservedInstanceBoughtInfo> lookupReservedInstanceBoughtInfos(long masterAccountId,
                                                                              long zoneId) {
        if (zoneId < 0) {
            logger.warn("lookupReservedInstanceBoughtInfos() masterAccountId={} zoneId={} < 0!",
                masterAccountId, zoneId);
            return null;
        }
        return reservedInstanceBoughtInfoTable.get(masterAccountId, zoneId);
    }

    /**
     * Access the reservedInstanceBoughtTable to return existing regional reserved instances for a
     * regional context and a dictionary for cloud entities.
     *
     * @param regionalContext regional context
     * @return list of regional RIs in the business account.
     */
    @Nonnull
    public List<ReservedInstanceBoughtInfo>
    lookupReservedInstancesBoughtInfos(ReservedInstanceRegionalContext regionalContext) {
        List<ReservedInstanceBoughtInfo> reservedInstances = new ArrayList<>();
        Map<Long, List<ReservedInstanceBoughtInfo>> risBought =
                            reservedInstanceBoughtInfoTable.rowMap().get(regionalContext.getMasterAccountId());
        long regionId = regionalContext.getRegionId();
        for (Entry<Long, List<ReservedInstanceBoughtInfo>> entry: risBought.entrySet()) {
            Long zone = entry.getKey();
            if (zone == null || zone == 0) {  // zone == 0, means regional RI
                List<ReservedInstanceBoughtInfo> infos = entry.getValue();
                for (ReservedInstanceBoughtInfo info: infos) {
                    long specId = info.getReservedInstanceSpec();
                    ReservedInstanceSpec spec = lookupReservedInstanceSpecWithId(specId);
                    if (spec == null) {
                        logger.error("lookupReservedInstancesBoughtInfos() spec not found for ID={}",
                            specId);
                        continue;
                    }
                    if (spec.getReservedInstanceSpecInfo().getRegionId() == regionId) {
                        reservedInstances.add(info);
                    }
                }
            }
        }
        return reservedInstances;
    }

    /**
     * Populate the reservedInstanceBoughtInfoTable with the reserved instance bought from the riBoughtStore.
     *
     * @param store the Reserved Instance bought store.  Pass in to ease JUnit testing.
     */
    @VisibleForTesting
    void populateReservedInstanceBoughtInfoTable(ReservedInstanceBoughtStore store) {
        // Table: business account OID X availability zone OID -> ReservedInstanceBoughtInfo
        reservedInstanceBoughtInfoTable =
            HashBasedTable.create();
        store.getReservedInstanceBoughtByFilter(
            // no special filter. get all records
            ReservedInstanceBoughtFilter.newBuilder()
                .build())
            .stream()
            .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
            .forEach(riBought -> {
                List<ReservedInstanceBoughtInfo> existingValue =
                    reservedInstanceBoughtInfoTable.get(riBought.getBusinessAccountId(), riBought.getAvailabilityZoneId());
                if (existingValue == null) {
                    existingValue = new ArrayList<>();
                }
                existingValue.add(riBought);
                reservedInstanceBoughtInfoTable.put(riBought.getBusinessAccountId(), riBought.getAvailabilityZoneId(),
                    existingValue);
            });
        logger.info("populateReservedInstanceBoughtInfoTable() size={}",
            reservedInstanceBoughtInfoTable.size());
    }

    /**
     * Access the reservedInstanceSpecIdMap to look up ReservedInstanceSpec.
     *
     * @param context region context
     * @param constraints purchasing constraints
     * @return specification of a reserved instance
     */
    @Nullable
    public ReservedInstanceSpec lookupReservedInstanceSpec(ReservedInstanceRegionalContext context,
                                                           ReservedInstancePurchaseConstraints constraints) {
        ReservedInstanceSpecKey key = new ReservedInstanceSpecKey(context, constraints);
        return reservedInstanceSpecKeyMap.get(key);
    }

    /**
     * Populate the reservedInstanceSpecKeyMap with the ReservedInstanceSpecs from the
     * ReservedInstanceSpecStore.


     * This method takes the instance variable, riSpecStore, as a parameter to make JUnit
     * testing easier.
     *
     * @param store the Reserved Instance spec store.  Passed in to ease JUnit testing.
     */
    @VisibleForTesting
    void populateReservedInstanceSpecKeyMap(ReservedInstanceSpecStore store) {
        Map<ReservedInstanceSpecKey, ReservedInstanceSpec> map = new HashMap<>();
        for (ReservedInstanceSpec spec: store.getAllReservedInstanceSpec()) {
            OSType osType = spec.getReservedInstanceSpecInfo().getOs();
            Tenancy tenancy = spec.getReservedInstanceSpecInfo().getTenancy();
            long regionId = spec.getReservedInstanceSpecInfo().getRegionId();
            long tierId = spec.getReservedInstanceSpecInfo().getTierId();
            ReservedInstancePurchaseConstraints constraints =
                new  ReservedInstancePurchaseConstraints(spec.getReservedInstanceSpecInfo().getType());
            ReservedInstanceSpecKey key = new ReservedInstanceSpecKey(regionId, tierId, osType,
                tenancy, constraints);
            if (map.get(key) == null) {
                map.put(key, spec);
            } else {
                // There is the same spec ID with and without size_flexible attribute.
                logger.trace("populateReservedInstanceSpecKeyMap() key={} has two specs: spec1={} spec2={}",
                    key, spec, map.get(key));
            }
        }
        reservedInstanceSpecKeyMap = map;
        logger.info("populateReservedInstanceSpecKeyMap() size={}", reservedInstanceSpecKeyMap.size());
    }

    /**
     * A class to encapsulate the on-demand and reserved instance price for a instance type.
     */
    public class PricingProviderResult {
        // The hourly on-demand price for an instance of some type.
        private final float onDemandRate;

        // The effective hourly price (actual hourly + amortized up-front) cost.
        private final float reservedInstanceRate;

        /**
         * Provide the on-demand and reserved instance rates.
         *
         * @param onDemandRate on-demand rate
         * @param riRate  reserved instance rate
         */
        public PricingProviderResult(float onDemandRate, float riRate) {
            this.onDemandRate = onDemandRate;
            this.reservedInstanceRate = riRate;
        }

        public float getOnDemandRate() {
            return onDemandRate;
        }

        public float getReservedInstanceRate() {
            return reservedInstanceRate;
        }
    }

    /**
     * Class to represent a ReservedInstanceSpec key.
     */
    public static class ReservedInstanceSpecKey {
        private final long regionId;
        private final long computeTierId;
        private final OSType osType;
        private final Tenancy tenancy;
        private final ReservedInstancePurchaseConstraints constraints;

        /**
         * Constructor of a Reserved instance specification key.
         *
         * @param regionId  region
         * @param computeTierId compute tier
         * @param osType platform
         * @param tenancy tenancy
         * @param constraints purchase constraints
         */
        public ReservedInstanceSpecKey(long regionId,
                                       long computeTierId,
                                       OSType osType,
                                       Tenancy tenancy,
                                       ReservedInstancePurchaseConstraints constraints) {
            this.regionId = regionId;
            this.computeTierId = computeTierId;
            this.osType = osType;
            this.tenancy = tenancy;
            this.constraints = constraints;
        }

        /**
         * Constructor of a Reserved instance specification key.
         *
         * @param context regional context
         * @param constraints purchase constraints
         */
        public ReservedInstanceSpecKey(ReservedInstanceRegionalContext context,
                                       ReservedInstancePurchaseConstraints constraints) {
            this.regionId = context.getRegionId();
            this.computeTierId = context.getComputeTier().getOid();
            this.osType = context.getPlatform();
            this.tenancy = context.getTenancy();
            this.constraints = constraints;
        }

        public long getRegionId() {
            return regionId;
        }

        public long getComputeTierId() {
            return computeTierId;
        }

        public OSType getOsType() {
            return osType;
        }

        public Tenancy getTenancy() {
            return tenancy;
        }

        public ReservedInstancePurchaseConstraints getConstraints() {
            return constraints;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof ReservedInstanceSpecKey)) {
                return false;
            }
            ReservedInstanceSpecKey key = (ReservedInstanceSpecKey)object;
            if (this.regionId == key.getRegionId() &&
                this.computeTierId == key.getComputeTierId() &&
                this.osType == key.getOsType() &&
                this.tenancy == key.getTenancy() &&
                this.constraints.equals(key.getConstraints())) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(regionId, computeTierId, osType.ordinal(), tenancy.ordinal(),
                constraints.hashCode());
        }

        @Override
        public String toString() {
            StringBuffer buffer = new StringBuffer();
            buffer.append("regionId=").append(regionId)
                .append(" computeTierId=").append(computeTierId)
                .append(" osType=").append(osType.name())
                .append(" tenancy=").append(tenancy.name())
                .append(" constraints=").append(constraints);
            return buffer.toString();
        }
    }
}
