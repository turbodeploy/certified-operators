package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;

/**
 * The class processes a analysis cluster which contains historical demand data from cost DB to
 * generate demand for the RI Buy Algorithm and RI Buy Chart in the RI Buy action. While the demand
 * for the RI Buy Algorithm is normalized by the smallest template in the family, the demand for the RI Buy Chart is not normalized.
 * The class takes in a regionalContext and its corresponding zonal contexts and together they are
 * called a cluster. On this cluster the two public methods getRIBuyDemand and getRIBuyChartDemand
 * perform a set of steps in a particular order to generate the processed demand.
 */
public class ReservedInstanceDataProcessor {

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

    /**
     * log tag for this recommendation.
     */
    private final String logTag;

    /**
     * Facade to access the historical demand.
     */
    private ReservedInstanceAnalyzerHistoricalData historicalData;

    /**
     * Refers to same instance type for non ISF. Refers to the smallest template
     * in the family for ISF.
     */
    private ReservedInstanceRegionalContext regionalContext;

    /**
     * A list of zonal contexts associated with the regional context.
     */
    private List<ReservedInstanceZonalContext> zonalContexts;

    /**
     * The scope of this analysis.
     */
    private ReservedInstanceAnalysisScope scope;

    /**
     * Type of data used in RI Buy Analysis -- ALLOCATION or CONSUMPTION based.
     */
    private ReservedInstanceHistoricalDemandDataType demandDataType;

    /**
     * The cloud topology.
     */
    private TopologyEntityCloudTopology cloudTopology;

    /*
     * Facade to access the rate and reserved instances.
     */
    private ReservedInstanceAnalyzerRateAndRIs rateAndRIProvider;

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
     * A map of RIs to a tracker object that captures to what extent an RI
     * has already been applied in other contexts.
     */
    private Map<ReservedInstanceBoughtInfo, ReservedInstanceAdjustmentTracker> riAdjustments =
            new HashMap<>();

    /**
     * Temp structure in which we hold data after fetching it from DB and and apply zonal & regional
     * RIs. getRIBuyChartDemand and getRIBuyDemand then use this structure to return their respective
     * outputs.
     */
    private Map<ReservedInstanceZonalContext, float[]> processedData = null;

    /**
     * Public Constructor.
     *
     * @param logTag            A unique string to identify related messages for a particular RI Buy analysis
     * @param historicalData    Facade to access the historical demand.
     * @param regionalContext   The regional context of the analysis cluster.
     * @param zonalContexts     A list of zonal contexts in the analysis cluster.
     * @param scope             The scope of this analysis.
     * @param demandDataType    Type of data used in RI Buy Analysis -- ALLOCATION or CONSUMPTION based.
     * @param cloudTopology     The cloud topology.
     * @param rateAndRIProvider Facade to access the rate and reserved instances.
     * @param demandWeight      The weight of the current value when added to the  historical data.
     */
    public ReservedInstanceDataProcessor(@Nonnull String logTag,
        @Nonnull ReservedInstanceAnalyzerHistoricalData historicalData,
                                         @Nonnull ReservedInstanceRegionalContext regionalContext,
                                         @Nonnull List<ReservedInstanceZonalContext> zonalContexts,
                                         @Nonnull ReservedInstanceAnalysisScope scope,
                                         @Nonnull ReservedInstanceHistoricalDemandDataType demandDataType,
                                         @Nonnull TopologyEntityCloudTopology cloudTopology,
                                         @Nonnull ReservedInstanceAnalyzerRateAndRIs rateAndRIProvider,
                                         float demandWeight) {
        this.logTag = Objects.requireNonNull(logTag);
        this.historicalData = Objects.requireNonNull(historicalData);
        this.regionalContext = Objects.requireNonNull(regionalContext);
        this.zonalContexts = Objects.requireNonNull(zonalContexts);
        this.scope = Objects.requireNonNull(scope);
        this.demandDataType = Objects.requireNonNull(demandDataType);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.rateAndRIProvider = Objects.requireNonNull(rateAndRIProvider);
        this.demandWeight = demandWeight;
        currentHour = roundDownToHour(System.currentTimeMillis());
        currentSlot = calculateSlot(currentHour);
    }

    @VisibleForTesting
    protected ReservedInstanceDataProcessor() {
        logTag = "DummyLogTag";
        currentHour = roundDownToHour(System.currentTimeMillis());
        demandWeight = 0.6f;
        currentSlot = calculateSlot(currentHour);
        demandDataType = ReservedInstanceHistoricalDemandDataType.ALLOCATION;
    }

    /**
     * Returns the data to be displayed in the Buy RI Action graph.
     * The steps are
     *  1. Get the data from the DB for each zonal context.
     *  2. Normalize(see normalizeDemand method) the demand for each zonal context.
     *  3. Apply Zonal RI's
     *  4. Apply regional RI's.
     *  5. Multiply each zonal demand by its corresponding coupon value.
     * @return A mapping between a template and its demand.
     */
    public Map<TopologyEntityDTO, float[]> getRIBuyChartDemand() {
        Map<ReservedInstanceZonalContext, float[]> demandByZonalContext  = computeAndCacheProcessedData();

        final Map<TopologyEntityDTO, float[]> chartDemand =
                getCouponDemandByTemplate(demandByZonalContext, regionalContext.computeTier
                        .getTypeSpecificInfo().getComputeTier().getNumCoupons());

        return chartDemand;
    }

    /**
     * Returns the data to be fed into the RI Buy Algorithm. There are a set of steps one after another
     * towards the end of which the data to be fed into the RI Buy Algorithm is ready.
     * The steps are
     *  1. Get the data from the DB for each zonal context.
     *  2. Normalize the demand for each zonal context.
     *  3. Apply Zonal RI's
     *  4. Apply regional RI's.
     *  5. Collapse the demand by each zonal context into single demand.
     * @return A float array containing the demand.
     */
    public float[] getRIBuyDemand() {
        Map<ReservedInstanceZonalContext, float[]> demandByZonalContext  = computeAndCacheProcessedData();

        return combineDemand(demandByZonalContext);
    }

    /**
     * Retrieves and processes the data to be fed into the Buy RI Algo. and the Buy RI chart.
     * The steps are
     *  1. Get the data from the DB for each zonal context.
     *  2. Normalize(see normalizeDemand method) the demand for each zonal context.
     *  3. Apply Zonal RI's
     *  4. Apply regional RI's.
     *
     * @return returns mapping of zonal context to demand with negative values.
     */
    private Map<ReservedInstanceZonalContext, float[]> computeAndCacheProcessedData() {
        if (processedData == null) {
            final Map<ReservedInstanceZonalContext, float[]> demandFromDB =
                    getDemandFromDB(scope, zonalContexts, demandDataType);

            final Map<ReservedInstanceZonalContext, float[]> demandByZonalContext
                    = normalizeDemand(demandFromDB, regionalContext);

            applyZonalRIsToCluster(demandByZonalContext, regionalContext, rateAndRIProvider);

            applyRegionalRIs(demandByZonalContext, regionalContext, rateAndRIProvider, cloudTopology.getEntities());

            processedData = demandByZonalContext;
        }
        return processedData;
    }

    /**
     * Takes in a mapping of zonal contexts and their coupon based demand (normalized by regional
     * coupons) and returns a mapping grouped by compute tier and demand (denormalized by regional
     * coupons).
     *
     * @param demandByZonalContext A mapping between the zonal context and its demand.
     * @param regionCoupons the number of regional coupons for the template type of the demand.
     *                      (The reason we use this is, in a previous step in "normalizeDemand" method
     *                      regionCoupons is being used to normalize the demand. So inorder to reverse
     *                      the effect of that we multiply the demand with the same regionCoupons)
     * @return Returns a mapping between different templates and their demand.
     */
    protected Map<TopologyEntityDTO, float[]> getCouponDemandByTemplate(Map<ReservedInstanceZonalContext,
                                                                     float[]> demandByZonalContext,
                                                                     int regionCoupons) {
        final Map<TopologyEntityDTO, float[]> chartDemand = new HashMap<>();

        for (Entry<ReservedInstanceZonalContext, float[]> entry : demandByZonalContext.entrySet()) {
            final ReservedInstanceZonalContext zonalContext = entry.getKey();
            final TopologyEntityDTO computeTier = zonalContext.getComputeTier();
            final float[] demand = entry.getValue();

            float[] demandInCoupons = chartDemand.get(computeTier);

            if (demandInCoupons == null) {
                demandInCoupons = new float[demand.length];
                Arrays.fill(demandInCoupons, 0f);
            }

            for (int i = 0; i < demandInCoupons.length; i++) {
                demandInCoupons[i] = demandInCoupons[i] + Math.max(0.0f, demand[i] * regionCoupons);
            }
            chartDemand.put(computeTier, demandInCoupons);
        }
        return chartDemand;
    }


    /**
     * Gets the demand for a cluster partitioned by zonal context.
     * Getting data from database, the array may contain "-1" values.
     * Want the number of coupons relative to profile, the instance type buying for.
     * For example, "c4" family, minimum size if 'large', which is 8 coupons, want the
     * number of coupons normalized to 8.
     * We use regional context profile coupons to handle instance size flexible.
     * Constraint: context.getInstanceType().getNumberOfCoupons() >=profile.getNumberOfCoupons()
     *
     * @param dBDemand a mapping between zonal context and their demand from DB.
     * @param regionalContext  the scope of analysis, a regional context.
     * @return mapping from zonalContext to normalized demand.
     */
    Map<ReservedInstanceZonalContext, float[]> normalizeDemand(Map<ReservedInstanceZonalContext, float[]> dBDemand,
                                                               ReservedInstanceRegionalContext regionalContext) {

        Map<ReservedInstanceZonalContext, float[]> demands = new HashMap<>();
        // why isn't the next line: regionCoupons = regionalContext.getComputeTier().getNumCoupons();
        int regionCoupons = regionalContext.getComputeTier().getTypeSpecificInfo().getComputeTier().getNumCoupons();
        for (Entry<ReservedInstanceZonalContext, float[]> entry : dBDemand.entrySet()) {
            float[] demand = entry.getValue();
            ReservedInstanceZonalContext zonalContext = entry.getKey();
            int normalizedCoupons = 1;
            int zonalCoupons = zonalContext.getComputeTier().getTypeSpecificInfo().getComputeTier().getNumCoupons();
            if (regionCoupons <= 0 || zonalCoupons < regionCoupons) {
                logger.error("{}context.getInstanceType().getNumberOfCoupons() {} < profile" +
                                ".getNumberOfCoupons() {} for {}",
                        logTag, zonalCoupons, regionCoupons, zonalContext.toString());
            } else {
                normalizedCoupons = zonalCoupons / regionCoupons;
            }
            demand = multiplyByCoupons(demand, normalizedCoupons);

            if (demand != null) {
                demands.put(zonalContext, demand);
            }
        }
        return demands;
    }

    /**
     * Multiplies the non zero demand with the coupons.
     *
     * @param demand The demand for a context in workload.
     * @param coupons value in coupons for the template.
     * @return The demand in terms of coupons.
     */
    protected float[] multiplyByCoupons(float[] demand, int coupons) {
        boolean foundValue = false;
        for (int i = 0; i < demand.length; i++) {
            if (demand[i] > 0) {
                // do not multiply un-populated data by number of coupons.
                demand[i] *= coupons;
                foundValue = true;
            }
        }
        if (foundValue) {
            return demand;
        }
        return null;
    }

    /**
     * Return demand adjusted by regional RIs.
     *
     * @param demandByZonalContext historical demand, in coupons normalized by regional context's compute tier.
     * @param regionalContext scope of analysis cluster.
     * @param rateAndRIProvider provider of rates and RIs.
     * @param cloudEntities dictionary for cloud entities.
     */
    void applyRegionalRIs(Map<ReservedInstanceZonalContext, float[]> demandByZonalContext,
                          @Nonnull ReservedInstanceRegionalContext regionalContext,
                          @Nonnull ReservedInstanceAnalyzerRateAndRIs rateAndRIProvider,
                          @Nonnull Map<Long, TopologyEntityDTO> cloudEntities) {

        List<ReservedInstanceBoughtInfo> risBought =
                rateAndRIProvider.lookupReservedInstancesBoughtInfos(regionalContext, logTag);
        if (risBought == null) {
            // log message generated in rateAndRIProvider.
            return;
        }
        ComputeTierInfo computeTierInfo = regionalContext.getComputeTier().getTypeSpecificInfo().getComputeTier();
        String regionalContextFamily = computeTierInfo.getFamily();
        int buyComputeTierCoupons = computeTierInfo.getNumCoupons();

        float normalizedCoupons = 0;
        for (ReservedInstanceBoughtInfo boughtInfo: risBought) {
            if (!boughtInfo.hasBusinessAccountId()) {
                logger.warn("{}applyRegionalRIs boughtInfo={} has no business account in regionalContext={}",
                    logTag, boughtInfo, regionalContext);
                continue;
            }
            ReservedInstanceSpec riSpec =
                    rateAndRIProvider.lookupReservedInstanceSpecWithId(boughtInfo.getReservedInstanceSpec());
            if (riSpec == null) {
                logger.warn("{}applyRegionalRIs riSpec == null for boughtInfo={} in regionalContext={}",
                    logTag, boughtInfo, regionalContext);
                continue;
            }
            ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
            if (riSpecInfo.getRegionId() == regionalContext.getRegionId()
                    // might have to look up master account for boughtInfo
                    && boughtInfo.getBusinessAccountId() == regionalContext.getMasterAccountId()
                    && riSpecInfo.getTenancy() == regionalContext.getTenancy()
                    && riSpecInfo.getOs() == regionalContext.getPlatform()
                    && (regionalContext.isInstanceSizeFlexible() ?
                    (Objects.equals(regionalContextFamily, cloudEntities.get(riSpecInfo.getTierId())
                            .getTypeSpecificInfo().getComputeTier().getFamily()))
                    : riSpecInfo.getTierId() == regionalContext.getComputeTier().getOid())
                    && (buyComputeTierCoupons > 0)) {
                normalizedCoupons =
                        boughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons() /
                                (float)buyComputeTierCoupons;
            }

            if (normalizedCoupons > 0) {
                Map<ReservedInstanceZonalContext, float[]> scopedDemandByZonalContext = new LinkedHashMap<>();
                // If the RI is dedicated we want to apply it to the correct member account.
                // scopedDemandByZonalContext is a subset of demandByZonalContext and contains entries
                // belonging to the same member account as the dedicated RI being applied.
                // If it is a shared RI then it contains all entries.
                for (Entry<ReservedInstanceZonalContext, float[]> entry :
                        demandByZonalContext.entrySet()) {
                    ReservedInstanceZonalContext context = entry.getKey();

                    if (!boughtInfo.getReservedInstanceScopeInfo().getShared() &&
                            !boughtInfo.getReservedInstanceScopeInfo().getApplicableBusinessAccountIdList()
                                    .contains(context.getMasterAccountId())) {
                        continue;
                    } else {
                        scopedDemandByZonalContext.put(context, entry.getValue());
                    }
                }
                subtractCouponsFromDemand(scopedDemandByZonalContext, normalizedCoupons, boughtInfo);
            }
        }
    }

    /**
     * Apply zonal RI's with the demand. The instance type in the context may be different than the
     * instance type in the cluster if instance size flexible.
     *
     * @param zonalContextDemands a map of from zonal context to demand. This method may modify the
     *                            demand portion of this parameter.
     * @param regionalContext     the scope of analysis, regionalContext.
     * @param priceAndRIProvider  Facade to access the rate and reserved instances.
     */
    @Nullable
    void applyZonalRIsToCluster(@Nonnull Map<ReservedInstanceZonalContext, float[]> zonalContextDemands,
                                   @Nonnull ReservedInstanceRegionalContext regionalContext,
                                   @Nonnull ReservedInstanceAnalyzerRateAndRIs priceAndRIProvider) {
        for (ReservedInstanceZonalContext zonalContext : zonalContextDemands.keySet()) {
            applyZonalRIs(zonalContextDemands.get(zonalContext), zonalContext,
                    regionalContext.getComputeTier(), priceAndRIProvider);

        }
    }

    /**
     * Apply zonal RI with the demand. The instance type in the context may be different than the
     * instance type in the cluster(mapping from regional context to a list of zonal contexts) if
     * instance size flexible.
     *
     * @param demand                the normalized demand for the zonal context. This method may modify
     *                              the demand portion of this parameter.
     * @param zonalContext          the zonal context.
     * @param buyComputeTier        The compute tier of the zonal context.
     * @param priceAndRIProvider    Facade to access the rate and reserved instances.
     */
    void applyZonalRIs(@Nullable float[] demand,
                          @Nonnull ReservedInstanceZonalContext zonalContext,
                          @Nonnull TopologyEntityDTO buyComputeTier,
                          @Nonnull ReservedInstanceAnalyzerRateAndRIs priceAndRIProvider) {
        List<ReservedInstanceBoughtInfo> risBought =
                priceAndRIProvider.lookupReservedInstanceBoughtInfos(zonalContext.getMasterAccountId(),
                        zonalContext.getAvailabilityZoneId(), logTag);

        if (risBought == null) {
            logger.debug("{}no RIs found in zonalContext={}", logTag, zonalContext);
            return;
        }
        for (ReservedInstanceBoughtInfo riBought : risBought) {
            ReservedInstanceSpec riSpec =
                    priceAndRIProvider.lookupReservedInstanceSpecWithId(riBought.getReservedInstanceSpec());
            if (riSpec == null) {
                logger.warn("{}applyZonalRIs riSpec == null for boughtInfo={} in zonalContext={}",
                    logTag, riBought, zonalContext);
                continue;
            }
            ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();

            if (riSpecInfo.getTenancy() == zonalContext.getTenancy()
                    && riSpecInfo.getOs() == zonalContext.getPlatform()
                    && riSpecInfo.getTierId() == zonalContext.getComputeTier().getOid()) {
                int normalizedCoupons = (riBought.getReservedInstanceBoughtCoupons().getNumberOfCoupons() /
                        buyComputeTier.getTypeSpecificInfo().getComputeTier().getNumCoupons());
                if (normalizedCoupons == 0) {
                    logger.debug("{}applyZonalRIs() no zonal RIs applicable in zonalContext={}",
                        logTag, zonalContext);
                    return;
                }

                Map<ReservedInstanceZonalContext, float[]> demandByZonalContext = new HashMap<>();
                demandByZonalContext.put(zonalContext, demand);
                subtractCouponsFromDemand(demandByZonalContext, normalizedCoupons, riBought);
            }
        }
    }

    /**
     * Combines multiple demand histories. For example, to add together zone
     * demand to get regional demand or to add instance types across a region.
     * All arrays must be of the same length.
     * Because the database can return an array with values of -1, to indicate the data has never been populated,
     * this routine does not remove -1 data values.
     *
     * @param demandByZonalContext a mapping between zonal context and their demand.
     * @return the combined demand.
     */
    protected float[] combineDemand(@Nullable Map<ReservedInstanceZonalContext, float[]> demandByZonalContext) {
        if (demandByZonalContext == null || demandByZonalContext.isEmpty()) {
            return null;
        }

        // assumption all arrays are of the same length.
        int length = demandByZonalContext.values().stream().findFirst().get().length;

        for (float[] demandHistory : demandByZonalContext.values()) {
            assert length == demandHistory.length;
        }

        // combined result may contain "-1" values to ensure proper combining.
        float[] combinedResult = new float[length];
        Arrays.fill(combinedResult, -1f);  // initialize to unpopulated.

        boolean foundData = false;
        for (float[] demandHistory : demandByZonalContext.values()) {
            for (int i = 0; i < demandHistory.length; i++) {
                // Only write non negative values to result.
                if (demandHistory[i] >= 0) { // do not add unpopulated data (-1).
                    foundData = true;
                    if (combinedResult[i] < 0f) {  // if unpopulated, set to demand history
                        combinedResult[i] = demandHistory[i];
                    } else {
                        combinedResult[i] += demandHistory[i];
                    }
                }
            }
        }

        if (!foundData) {
            return null;
        } else {
            return combinedResult;
        }
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
     * @param demandByZonalContext The demand data to be adjusted for the RI. In this method we
     *                            perform operations on this parameter and may alter it.
     * @param normalizedRICoupons The number of coupons of demand the RI could offset.
     * @param riBought The RI to adjust for.
     */
    protected void subtractCouponsFromDemand(Map<ReservedInstanceZonalContext, float[]> demandByZonalContext,
                                      float normalizedRICoupons,
                                      @Nonnull ReservedInstanceBoughtInfo riBought) {
        Calendar riPurchaseHour = roundDownToHour(riBought.getStartTime());
        int purchaseWeeksAgo = weeksAgo(currentHour, riPurchaseHour);

        /**
         * In consumption mode, it's unless the current hours is the same hour
           of the week as the hour when the RI was purchased, some hours will
           have been updated one more time than others, and thus need to
           be adjusted by a smaller amount (see explanation of calculation below).
           Thus there are two amounts that may apply, a larger and a smaller one.
           In allocation mode, both are just the full amount of the RI's
           normalized coupons.
         */
        final float largeAdjustment;
        final float smallAdjustment;

        /**
         * Arrays of adjustment for every hour of the week, which will either
           be initialized to the above, or which may be obtained from a tracker
           representing the remainder of an adjustment that may have been
           "used up" in whole or part for a different platform.
         */
        final float[] largeAdjustmentArray;
        final float[] smallAdjustmentArray;
        ReservedInstanceAdjustmentTracker reservedInstanceAdjustmentTracker =
                riAdjustments.get(riBought);
        if (reservedInstanceAdjustmentTracker == null) {
            if (this.demandDataType == ReservedInstanceHistoricalDemandDataType.ALLOCATION) {
                /**
                  No RIs were subtracted from the demand at the time of recording,
                  so we need to remove the entire RI now. For plans, filtering out of RIs
                  based on expiry date override will already have been
                  done in ChangeLoadObjectImpl, there is no need to check RIs for expiry here.
                */
                largeAdjustment = smallAdjustment = normalizedRICoupons;
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
                largeAdjustment = (float)Math.pow(1.0f - demandWeight, purchaseWeeksAgo)
                        * normalizedRICoupons;
                smallAdjustment = (float)Math.pow(1.0f - demandWeight, purchaseWeeksAgo + 1)
                        * normalizedRICoupons;
            }

            largeAdjustmentArray = new float[WEEKLY_DEMAND_DATA_SIZE];
            smallAdjustmentArray = new float[WEEKLY_DEMAND_DATA_SIZE];
            Arrays.fill(largeAdjustmentArray, largeAdjustment);
            Arrays.fill(smallAdjustmentArray, smallAdjustment);
            reservedInstanceAdjustmentTracker = new ReservedInstanceAdjustmentTracker(
                    smallAdjustmentArray, largeAdjustmentArray, smallAdjustment, largeAdjustment);
            riAdjustments.put(riBought, reservedInstanceAdjustmentTracker);
            logger.debug("{}Creating adjustments for RI: {} for the contexts: {}, " +
                            "largeAdjustment: {}, smallAdjustment: {}, Total " +
                            "largeAdjustment: {}, Total smallAdjustment: {}", logTag,
                riBought.getDisplayName(), demandByZonalContext.keySet(), largeAdjustment,
                smallAdjustment, sum(largeAdjustmentArray), sum(smallAdjustmentArray));
        } else {
            logger.debug("{}Reusing adjustments for RI: {} for the contexts: {}", logTag,
                riBought.getDisplayName(), demandByZonalContext.keySet());
            largeAdjustmentArray = reservedInstanceAdjustmentTracker.getLargeAdjustmentArray();
            smallAdjustmentArray = reservedInstanceAdjustmentTracker.getSmallAdjustmentArray();
            smallAdjustment = reservedInstanceAdjustmentTracker.getSmallAdjustment();
            largeAdjustment = reservedInstanceAdjustmentTracker.getLargeAdjustment();
        }

        int purchaseSlot = calculateSlot(riPurchaseHour);
        // If it's too small to matter, don't bother updating all the data points.
        if (largeAdjustment < ADJUSTMENT_CUTOFF) {
            logger.debug("{}No adjustment for RI {} bought {} weeks ago, effect would be only {} coupons",
                    logTag, riBought.getDisplayName(), purchaseWeeksAgo, largeAdjustment);

            return;
        }

        for (Entry<ReservedInstanceZonalContext, float[]> entry : demandByZonalContext.entrySet()) {
            float[] demand = entry.getValue();

            int slot = purchaseSlot;
            // Equal included since if they're the same slot we want to go all the way around
            if (slot <= currentSlot) {
                // Ensure we're greater and can index backwards to currentSlot.
                slot += WEEKLY_DEMAND_DATA_SIZE;
            }
            if (slot > currentSlot && logger.isDebugEnabled()) {
                logDemandAdjustment(currentHour, riPurchaseHour, purchaseWeeksAgo, largeAdjustment, riBought);
            }

            if (demand == null) {
                return;
            }

            /**
             * We cannot map day and hour into the correct index unless the full 24 * 7 points are
               present. Fortunately the history provider does this, giving -1 for hours with no data.
               If it just omitted them and made the array shorter, we wouldn't know which index
               to update for a given hour. This should not happen, but if it does, log
               the problem and continue with other contexts instead of throwing an
               IndexError and blowing up the whole analysis
             */
            if (demand.length < WEEKLY_DEMAND_DATA_SIZE) {
                logger.error("{}Demand array is the wrong size processing RI coverage for {}, skipping.",
                        logTag, riBought.getDisplayName());

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
                logDemandAdjustment(riPurchaseHour, currentHour, purchaseWeeksAgo, smallAdjustment, riBought);
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
    }

    /** Returns a mapping of zonal contexts with their demand from the DB.
     *
     * @param scope analysis scope.
     * @param zonalContexts a list of zonal contexts associated with the regional context.
     * @param demandDataType what demand to use for the analysis? E.g. allocation or consumption.
     * @return A mapping of zonal contexts with their demand from the DB.
     */
    private Map<ReservedInstanceZonalContext, float[]> getDemandFromDB(ReservedInstanceAnalysisScope scope,
                                                                       List<ReservedInstanceZonalContext> zonalContexts,
                                                                       ReservedInstanceHistoricalDemandDataType demandDataType) {
        Map<ReservedInstanceZonalContext, float[]> demands = new HashMap<>();
        for (ReservedInstanceZonalContext context : zonalContexts) {
            float[] demand = historicalData.getDemand(scope, context, demandDataType);
            demands.put(context, demand);
        }
        return demands;
    }


    private void logDemandAdjustment(Calendar from, Calendar to, int weeksAgo, float adjustment,
                                     ReservedInstanceBoughtInfo ri) {
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
}
