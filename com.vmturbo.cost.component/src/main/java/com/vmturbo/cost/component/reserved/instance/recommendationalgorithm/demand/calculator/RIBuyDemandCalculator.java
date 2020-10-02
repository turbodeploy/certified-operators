package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceHistoricalDemandDataType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyHistoricalDemandProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCache;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceInventoryMatcher;

/**
 * This calculator is responsible for merging recorded demand with a provided RI inventory, in order
 * to calculate any uncovered demand that can be subsequently analyzed. For consumption/projected
 * demand, the calculator assumes the recorded demand represents uncovered demand at the point the
 * demand was recorded. Therefore, for consumption demand only, only newly purchased RIs are considered
 * in subtracting covered demand from the aggregate.
 */
public class RIBuyDemandCalculator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * If the amount to adjust demand by is less than this many coupons,
     don't bother, the effect is too small to matter. This is chosen
     conservatively. 0.3 should be enough for any single RI purchase,
     but multiple purchases in the same context could add up to enough
     to matter.
     */
    private static final float ADJUSTMENT_CUTOFF = 0.01f;
    private static final long MS_PER_WEEK = TimeUnit.DAYS.toMillis(7);

    /**
     * The size of the demand data for from the compute_tier_hourly_by_week table. The size is total
     * number of hours per week: 168 = 24 * 7.
     */
    public static final int WEEKLY_DEMAND_DATA_SIZE = 168;

    private final RIBuyHistoricalDemandProvider demandProvider;

    /**
     * Type of data used in RI Buy Analysis -- ALLOCATION or CONSUMPTION based.
     */
    private ReservedInstanceHistoricalDemandDataType demandDataType;

    private RegionalRIMatcherCache regionalRIMatcherCache;

    /**
     * The weight of the current value when added to the historical data.
     */
    private float demandWeight;

    /**
     * Represents when analysis is being conducted, used for determining
       how long ago an RI was purchased and how much of its effect to
       remove from demand data.
     */
    private Calendar currentHour;

    /**
     * The index in the demand array for the current hour.
     */
    private int currentSlot;

    /**
     * Public Constructor.
     *
     * @param demandProvider        The provider of recorded demand based on a requested demand context
     * @param demandDataType        Type of data used in RI Buy Analysis -- ALLOCATION or CONSUMPTION based.
     * @param regionalRIMatcherCache The regional RI matcher cache
     * @param demandWeight          The weight of the current value when added to the  historical data.
     */
    public RIBuyDemandCalculator(
            @Nonnull RIBuyHistoricalDemandProvider demandProvider,
            @Nonnull ReservedInstanceHistoricalDemandDataType demandDataType,
            @Nonnull RegionalRIMatcherCache regionalRIMatcherCache,
            float demandWeight) {
        this.demandProvider = Objects.requireNonNull(demandProvider);
        this.demandDataType = Objects.requireNonNull(demandDataType);
        this.regionalRIMatcherCache = Objects.requireNonNull(regionalRIMatcherCache);
        this.demandWeight = demandWeight;
        currentHour = roundDownToHour(System.currentTimeMillis());
        currentSlot = calculateSlot(currentHour);
    }

    /**
     * Returns the data to be fed into the RI Buy Algorithm. There are a set of steps one after another
     * towards the end of which the data to be fed into the RI Buy Algorithm is ready.
     * The steps are
     *  1. Get the data from the DB for each zonal context.
     *  2. Convert the recorded demand to coupons
     *  3. Apply RI inventory to the recorded demand, based on the coverage rules for the associated
     *  provider.
     *  5. Group the demand by compute tier for the RI buy chart
     *  6. Normalize the demand to the target RI spec
     *
     * @param regionalContext The regional context.
     * @return A float array containing the demand.
     */
    public RIBuyDemandCalculationInfo calculateUncoveredDemand(
            @Nonnull RIBuyRegionalContext regionalContext) {

        // uncovered demand will be in coupons, based on the associated demand context
        logger.debug("Starting uncovered demand by cluster calculation for Region Name {} and region OID {}",
                        regionalContext.region().getDisplayName(), regionalContext.regionOid());
        final Map<RIBuyDemandCluster, float[]> uncoveredDemandByCluster  =
                computeUncoveredDemandByCluster(regionalContext);
        logger.debug("Ending uncovered demand by cluster calculation for Region Name {} and region OID {}",
                        regionalContext.region().getDisplayName(), regionalContext.regionOid());

        //used for the UI to graph demand by compute tier
        logger.debug("Starting uncovered demand by compute Tier calculation for Region Name {} and region OID {}",
                        regionalContext.region().getDisplayName(), regionalContext.regionOid());
        final Map<TopologyEntityDTO, float[]> uncoveredDemandByComputeTier =
                getCouponDemandByTemplate(uncoveredDemandByCluster);
        logger.debug("Ending uncovered demand by compute Tier calculation for Region Name {} and region OID {}",
                        regionalContext.region().getDisplayName(), regionalContext.regionOid());

        //used for the RI buy algorithm
        final float[] normalizedAggregateDemand = normalizeAndCombineDemand(
                regionalContext, uncoveredDemandByCluster);

        return ImmutableRIBuyDemandCalculationInfo.builder()
                .putAllUncoveredDemandByComputeTier(uncoveredDemandByComputeTier)
                .aggregateUncoveredDemand(normalizedAggregateDemand)
                .activeHours(countActive(normalizedAggregateDemand))
                .primaryAccountOid(determinePrimaryAccount(uncoveredDemandByCluster))
                .build();
    }

    /**
     * Retrieves and processes the data to be fed into the Buy RI Algorithm. and the Buy RI chart.
     * The steps are
     *  1. Get the data from the DB for each demand cluster.
     *  2. Converts the recorded demand to coupons.
     *  3. Apply RIs to calculate uncovered demand
     *
     * @param regionalContext The regional context.
     * @return returns mapping of demand cluster to demand with negative values.
     */
    private Map<RIBuyDemandCluster, float[]> computeUncoveredDemandByCluster(
            @Nonnull RIBuyRegionalContext regionalContext) {

        logger.debug("Starting demand collection from DB for Region Name {} and region OID {}",
                        regionalContext.region().getDisplayName(), regionalContext.regionOid());
        final Map<RIBuyDemandCluster, float[]> demandFromDB =
                regionalContext.demandClusters()
                        .stream()
                        .collect(Collectors.toMap(
                                Function.identity(),
                                demandContext -> demandProvider.getDemand(demandContext, demandDataType)));
        logger.debug("Ending demand collection from DB for Region Name {} and region OID {}",
                        regionalContext.region().getDisplayName(), regionalContext.regionOid());

        final Map<RIBuyDemandCluster, float[]> couponDemandByCluster =
                convertDemandToCoupons(demandFromDB);

        /*
        THIS IS A HACK - the RI allocation rules will eventually be moved to the service provider
        and therefore a determination of the CSP will not be required.
         */
        final boolean isAws = couponDemandByCluster.keySet().stream()
                .anyMatch(c -> c.regionOrZoneOid() != regionalContext.region().getOid());
        logger.debug("Starting applyReservedInstancesToCluster for Region Name {} and region OID {}",
                        regionalContext.region().getDisplayName(), regionalContext.regionOid());
        applyReservedInstancesToCluster(regionalContext, couponDemandByCluster, isAws);
        logger.debug("Ending applyReservedInstancesToCluster for Region Name {} and region OID {}",
                        regionalContext.region().getDisplayName(), regionalContext.regionOid());
        return couponDemandByCluster;
    }


    /**
     * Applies RI inventory to a cluster, based on a set of rules from {@link StaticRICoverageRules}.
     * Currently, the applicable coverage rules are determined by looking at the recorded location of
     * demand, with a zone indicating AWS, a region indicating Azure. This is a short-term solution, with
     * the expecation that these rules will be pushed to the service provider entity.
     *
     * <p>The adjustments/coverage from RI inventory is tracked across the demand contexts through
     * {@link ReservedInstanceAdjustmentTracker}.
     *
     * @param regionalContext       The regional context to analyze
     * @param couponDemandByCluster The recorded demand by cluster, in coupons
     * @param isAws                 Indicates whether this cluster is associated with AWS or Azure.
     */
    private void applyReservedInstancesToCluster(
            @Nonnull RIBuyRegionalContext regionalContext,
            @Nonnull Map<RIBuyDemandCluster, float[]> couponDemandByCluster,
            boolean isAws) {


        final List<RICoverageRule> riCoverageRules = isAws
                ? StaticRICoverageRules.AWS_RI_COVERAGE_RULES
                : StaticRICoverageRules.AZURE_RI_COVERAGE_RULES;
        final Map<Long, ReservedInstanceAdjustmentTracker> riAdjustmentsById = new HashMap<>();

        for (RICoverageRule coverageRule : riCoverageRules) {
            for (Map.Entry<RIBuyDemandCluster, float[]> demandContextEntry : couponDemandByCluster.entrySet()) {

                final ReservedInstanceInventoryMatcher riInventoryMatcher =
                        regionalRIMatcherCache.getOrCreateRIInventoryMatcherForRegion(
                                regionalContext.regionOid());
                final Set<ReservedInstanceBought> risInScope = riInventoryMatcher.matchToDemandContext(
                        coverageRule, regionalContext, demandContextEntry.getKey());

                risInScope.forEach(ri -> {

                    subtractCouponsFromDemand(
                            demandContextEntry.getValue(),
                            riAdjustmentsById,
                            ri, regionalContext.analysisTag());
                });
            }
        }
    }

    @Nonnull
    private Map<TopologyEntityDTO, float[]> getCouponDemandByTemplate(
            @Nonnull Map<RIBuyDemandCluster, float[]> demandContexts) {

        final Map<TopologyEntityDTO, float[]> chartDemand = new HashMap<>();

        for (Entry<RIBuyDemandCluster, float[]> entry : demandContexts.entrySet()) {
            final RIBuyDemandCluster demandCluster = entry.getKey();
            final TopologyEntityDTO computeTier = demandCluster.computeTier();
            final float[] demand = entry.getValue();

            float[] demandInCoupons = chartDemand.get(computeTier);

            if (demandInCoupons == null) {
                demandInCoupons = new float[demand.length];
                Arrays.fill(demandInCoupons, 0f);
            }

            for (int i = 0; i < demandInCoupons.length; i++) {
                demandInCoupons[i] = demandInCoupons[i] + Math.max(0.0f, demand[i]);
            }
            chartDemand.put(computeTier, demandInCoupons);
        }
        return chartDemand;
    }

    @Nonnull
    private Map<RIBuyDemandCluster, float[]> convertDemandToCoupons(
            Map<RIBuyDemandCluster, float[]> dBDemand) {

        final Map<RIBuyDemandCluster, float[]> demands = new HashMap<>();

        for (Entry<RIBuyDemandCluster, float[]> entry : dBDemand.entrySet()) {

            final RIBuyDemandCluster demandCluster = entry.getKey();
            final float[] demandInCoupons = multiplyByCoupons(
                    entry.getValue(),
                    demandCluster.computeTierCoupons());

            if (demandInCoupons != null) {
                demands.put(demandCluster, demandInCoupons);
            }
        }
        return demands;
    }

    @Nonnull
    private float[] multiplyByCoupons(float[] demand, int coupons) {
        final float[] couponDemand = new float[demand.length];
        for (int i = 0; i < demand.length; i++) {
            if (demand[i] > 0) {
                couponDemand[i] = demand[i] * coupons;
            } else {
                couponDemand[i] = demand[i];
            }
        }

        return couponDemand;
    }

    /**
     * Combines multiple demand histories. For example, to add together zone
     * demand to get regional demand or to add instance types across a region.
     * All arrays must be of the same length.
     * Because the database can return an array with values of -1, to indicate the data has never been populated,
     * this routine does not remove -1 data values.
     *
     * @param regionalContext The regional context.
     * @param demandByCluster a mapping between demand context and their demand.
     * @return the combined demand.
     */
    private float[] normalizeAndCombineDemand(
            @Nonnull RIBuyRegionalContext regionalContext,
            @Nullable Map<RIBuyDemandCluster, float[]> demandByCluster) {
        if (demandByCluster == null || demandByCluster.isEmpty()) {
            return null;
        }

        // assumption all arrays are of the same length.
        int length = demandByCluster.values().stream().findFirst().get().length;

        for (float[] demandHistory : demandByCluster.values()) {
            assert length == demandHistory.length;
        }

        // combined result may contain "-1" values to ensure proper combining.
        float[] combinedResult = new float[length];
        Arrays.fill(combinedResult, -1f);  // initialize to unpopulated.

        final int couponsPerRecommendedInstance = regionalContext.couponsPerRecommendedInstance();
        for (float[] demandHistory : demandByCluster.values()) {
            for (int i = 0; i < demandHistory.length; i++) {
                // Only write non negative values to result.
                if (demandHistory[i] >= 0) { // do not add unpopulated data (-1).
                    if (combinedResult[i] < 0f) {  // if unpopulated, set to demand history
                        combinedResult[i] = (demandHistory[i] / couponsPerRecommendedInstance);
                    } else {
                        combinedResult[i] += (demandHistory[i] / couponsPerRecommendedInstance);
                    }
                }
            }
        }

        return combinedResult;
    }

    /**
     * This method takes actual recorded demand data, and modifies it to represent what
     * WOULD have been recorded if the given RI had been in inventory all along. The goal is
     * to produce data that represents demand that remains uncovered now, on which
     * ReservedInstanceAnalyzer can base purchase decisions. If these adjustments were not
     * made, then the analyzer would recommend buying RIs over and over.
     *
     * <p>Consumption and Allocation demand is recorded differently and thus must be adjusted
     * differently.
     *
     * <p>Consumption demand records only on-demand use, which is to say that any RI-covered
     * demand in effect at the time of recording has already been subtracted. But, we need to
     * adjust for "recently" purchased RIs, since they will have been subtracted out of newer
     * updates but not older ones, and as explained above we need the data to look like what
     * would have been recorded if the RI had always been.
     *
     * <p>Consumption demand is recorded this way to avoid a feedback loop: the market resizes to
     * take advantage of the RIs we have, even if they're not ideal. The demand therefore
     * is biased by existing RI inventory and when RIs expire we tend to buy more of the same,
     * rather than considering what is actually ideal now.
     *
     * <p>The downside of the approach is that we cannot simulate the future expiration of RIs.
     * The effect of the RIs has already been subtracted from the demand, and we can't know
     * how much to add back in to simulate the RI having expired because we can't assume the
     * RI was 100% utilized.
     *
     * <p>For Allocation based analysis, we're already giving RI Buy recommendations based on the
     * VM instance types you have, rather than ideal, so the effect of bias is less important.
     * So, we choose the opposite trade-off and don't subtract RIs at recording time
     * so we can selectively remove them or not during analysis.
     *
     * @param demand the recorded demand, in coupons
     * @param riAdjustmentsById A map of RI ID to an adjustment tracker, in order to track RI coverage
     *                          applications across demand clusters.
     * @param riBought The RI to adjust for.
     * @param logTag A tag to use in logging.
     */
    private void subtractCouponsFromDemand(
            @Nonnull float[] demand,
            @Nonnull Map<Long, ReservedInstanceAdjustmentTracker> riAdjustmentsById,
            @Nonnull ReservedInstanceBought riBought,
            String logTag) {

        final ReservedInstanceBoughtInfo riInfo = riBought.getReservedInstanceBoughtInfo();
        final double riCoupons = riInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons();
        final Calendar riPurchaseHour = roundDownToHour(riInfo.getStartTime());
        final int purchaseWeeksAgo = weeksAgo(currentHour, riPurchaseHour);

        /*
           In consumption mode, it's unless the current hours is the same hour
           of the week as the hour when the RI was purchased, some hours will
           have been updated one more time than others, and thus need to
           be adjusted by a smaller amount (see explanation of calculation below).
           Thus there are two amounts that may apply, a larger and a smaller one.
           In allocation mode, both are just the full amount of the RI's
           normalized coupons.
         */
        final float largeAdjustment;
        final float smallAdjustment;

        /*
           Arrays of adjustment for every hour of the week, which will either
           be initialized to the above, or which may be obtained from a tracker
           representing the remainder of an adjustment that may have been
           "used up" in whole or part for a different platform.
         */
        final float[] largeAdjustmentArray;
        final float[] smallAdjustmentArray;
        ReservedInstanceAdjustmentTracker reservedInstanceAdjustmentTracker =
                riAdjustmentsById.get(riBought.getId());
        if (reservedInstanceAdjustmentTracker == null) {
            if (this.demandDataType == ReservedInstanceHistoricalDemandDataType.ALLOCATION) {
                /*
                  No RIs were subtracted from the demand at the time of recording,
                  so we need to remove the entire RI now. For plans, filtering out of RIs
                  based on expiry date override will already have been
                  done in ChangeLoadObjectImpl, there is no need to check RIs for expiry here.
                */
                largeAdjustment = smallAdjustment = (float)riCoupons;
            } else {
                /*
                 *  For consumption demand, RIs that existed at the time of recording were
                 *  subtracted out then, but new RIs could have been purchased, and we need to
                 *  adjust for those.
                 *
                 *  The amount to subtract for a data point for an RI of C coupons purchased K weeks
                 *  ago with weighting W is S = (1 - W)**K * C.
                 *
                 *  It's likely we have two different values for K over the course of the 168 data points,
                 *  unless the purchase happens in the same slot as the current hour (T), the hours
                 *  between now (T) and the purchase time (P) have been updated once more and thus
                 *  need a smaller adjustment than the other hours.
                 *
                 *  We can do this by starting at the slot for P and going back to just
                 *  before slot T, updating by largeAdjustment, and then from T back to
                 *  before P by smallAdjustment.
                 *
                 *  We include both P for the large adjustment assuming the update had not yet
                 *  run in that hour, as it's more conservative. In the degenerate case where
                 *  P and T are the same slot, we'll apply the larger adjustment for every hour.
                 *  In the case where P and T are literally the same hour (not just the same slot),
                 *  we remove the full demand (1 * C) from every hour.
                 */
                largeAdjustment = (float)(Math.pow(1.0f - demandWeight, purchaseWeeksAgo)
                        * riCoupons);
                smallAdjustment = (float)(Math.pow(1.0f - demandWeight, purchaseWeeksAgo + 1)
                        * riCoupons);
            }

            largeAdjustmentArray = new float[WEEKLY_DEMAND_DATA_SIZE];
            smallAdjustmentArray = new float[WEEKLY_DEMAND_DATA_SIZE];
            Arrays.fill(largeAdjustmentArray, largeAdjustment);
            Arrays.fill(smallAdjustmentArray, smallAdjustment);
            reservedInstanceAdjustmentTracker = new ReservedInstanceAdjustmentTracker(
                    smallAdjustmentArray, largeAdjustmentArray, smallAdjustment, largeAdjustment);
            riAdjustmentsById.put(riBought.getId(), reservedInstanceAdjustmentTracker);
        } else {
            largeAdjustmentArray = reservedInstanceAdjustmentTracker.getLargeAdjustmentArray();
            smallAdjustmentArray = reservedInstanceAdjustmentTracker.getSmallAdjustmentArray();
            smallAdjustment = reservedInstanceAdjustmentTracker.getSmallAdjustment();
            largeAdjustment = reservedInstanceAdjustmentTracker.getLargeAdjustment();
        }

        int purchaseSlot = calculateSlot(riPurchaseHour);
        // If it's too small to matter, don't bother updating all the data points.
        if (largeAdjustment < ADJUSTMENT_CUTOFF) {
            logger.debug("{}No adjustment for RI {} bought {} weeks ago, effect would be only {} coupons",
                    logTag, riInfo.getDisplayName(), purchaseWeeksAgo, largeAdjustment);

            return;
        }

        int slot = purchaseSlot;
        // Equal included since if they're the same slot we want to go all the way around
        if (slot <= currentSlot) {
            // Ensure we're greater and can index backwards to currentSlot.
            slot += WEEKLY_DEMAND_DATA_SIZE;
        }
        if (slot > currentSlot && logger.isDebugEnabled()) {
            logDemandAdjustment(currentHour, riPurchaseHour,
                    purchaseWeeksAgo, largeAdjustment, riInfo, logTag);
        }

        if (demand == null) {
            return;
        }

        /*
           We cannot map day and hour into the correct index unless the full 24 * 7 points are
           present. Fortunately the history provider does this, giving -1 for hours with no data.
           If it just omitted them and made the array shorter, we wouldn't know which index
           to update for a given hour. This should not happen, but if it does, log
           the problem and continue with other contexts instead of throwing an
           IndexError and blowing up the whole analysis
         */
        if (demand.length < WEEKLY_DEMAND_DATA_SIZE) {
            logger.error("{}Demand array is the wrong size processing RI coverage for {}, skipping.",
                    logTag, riInfo.getDisplayName());

            return;
        }

        for (; slot > currentSlot; slot--) {
            int index = slot % WEEKLY_DEMAND_DATA_SIZE;
            if (demand[index] > 0) {
                float val = demand[index];
                demand[index] = Math.max(val - largeAdjustmentArray[index], 0);
                largeAdjustmentArray[index] = Math.max(largeAdjustmentArray[index] - val, 0);
            }
        }

        // Equal NOT included since if they're the same slot we already went all the way around.
        if (slot < purchaseSlot) {
            // Ensure we're greater and can index backwards to purchaseSlot.
            slot += WEEKLY_DEMAND_DATA_SIZE;
        }
        if (slot > purchaseSlot && logger.isDebugEnabled()) {
            logDemandAdjustment(riPurchaseHour, currentHour,
                    purchaseWeeksAgo, smallAdjustment, riInfo, logTag);
        }
        for (; slot > purchaseSlot; slot--) {
            int index = slot % WEEKLY_DEMAND_DATA_SIZE;
            if (demand[index] > 0) {
                float val = demand[index];
                demand[index] = Math.max(val - smallAdjustmentArray[index], 0);
                smallAdjustmentArray[index] = Math.max(smallAdjustmentArray[index] - val, 0);
            }
        }
    }

    private void logDemandAdjustment(Calendar from, Calendar to,
                                     int weeksAgo, float adjustment,
                                     ReservedInstanceBoughtInfo ri,
                                     String logTag) {
        Locale locale = Locale.getDefault();

        logger.debug("{}Reducing demand in range ({} {}:00, {} {}:00] by {} coupons " +
                "for RI {} bought {} weeks ago",
            logTag, from.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.SHORT, locale),
            from.get(Calendar.HOUR_OF_DAY),
            to.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.SHORT, locale),
            to.get(Calendar.HOUR_OF_DAY),
            adjustment,
            ri.getDisplayName(),
            weeksAgo);
    }

    /**
     * Calculate the index into the demand array for a calendar object representing some hour.
     * Calendar class day-of-week runs 1..7, so one is subtracted to get an index
     * in the range [0, 167] covering one week of hours. Sunday is 0. This is consistent
     * with the way the way the HistoryProviderImpl maps hours from returned rows
     * into indices in the returned demand data.
     *
     * @param calendar A calendar object representing any hour.
     * @return the index into a demand array that would hold the slot for that hour.
     */
    protected static int calculateSlot(Calendar calendar) {
        return ((calendar.get(Calendar.DAY_OF_WEEK) - 1) * 24) + calendar.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * Given a point in time return a calendar object representing the start
     * of the hour.
     *
     * @param millis milliseconds since the epoch.
     * @return A calendar representing the start of the hour in which the time
     * represented by millis exists.
     */
    @Nonnull
    protected static Calendar roundDownToHour(long millis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);

        // zero hour remainder to round down to the start of the hour
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return calendar;
    }

    /**
     * Calculate how many weeks between two hours, rounded down.
     *
     * @param now The current (or newer) hour.
     * @param then A particular hour that is before 'now'.
     * @return the difference in weeks, rounded down, in the range
     * 0..n.
     */
    protected static int weeksAgo(Calendar now, Calendar then) {
        return (int)((now.getTimeInMillis() - then.getTimeInMillis()) / MS_PER_WEEK);
    }

    private float sum(float[] array) {
        float sum = 0;
        for (final float element : array) {
            sum += element;
        }
        return sum;
    }

    private int countActive(@Nonnull float[] normalizedDemand) {
        int activeHours = 0;
        for (int index = 0; index < normalizedDemand.length; index++) {
            activeHours += normalizedDemand[index] >= 0 ? 1 : 0;
        }
        return activeHours;
    }

    /**
     * Determines the primary account in the recorded demand by looking at the account with the most
     * recorded demand.
     *
     * @param uncoveredDemandByCluster The uncovered demand (in coupons), indexed by demand context which
     *                                 contains the associated account of the demand
     * @return The primary account OID.
     */
    private long determinePrimaryAccount(
            @Nonnull final Map<RIBuyDemandCluster, float[]> uncoveredDemandByCluster) {

        // First group aggregate demand by the account OID
        final Map<Long, Double> totalDemandByAccountOid = uncoveredDemandByCluster.entrySet()
                .stream()
                .collect(
                        Collectors.groupingBy(
                                e -> e.getKey().accountOid(),
                                // sum up all float[] demand for the grouped account OID
                                Collectors.summingDouble(
                                        e -> sum(e.getValue()))));

        // Select the entry with the max demand and return the account OID
        return Collections.max(
                totalDemandByAccountOid.entrySet(),
                Comparator.comparing(Map.Entry::getValue)).getKey();
    }
}
