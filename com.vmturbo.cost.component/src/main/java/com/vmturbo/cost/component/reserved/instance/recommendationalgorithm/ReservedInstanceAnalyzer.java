package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.reservedinstance.recommendationalgorithm.RecommendationKernelAlgorithm;
import com.vmturbo.commons.reservedinstance.recommendationalgorithm.RecommendationKernelAlgorithmResult;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.setting.CategoryPathConstants;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSender;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.RIBuyRateProvider.PricingProviderResult;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyAnalysisContextInfo;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyAnalysisContextProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator.RIBuyDemandCalculationInfo;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator.RIBuyDemandCalculator;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator.RIBuyDemandCalculatorFactory;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;

/**
 * <p>This class is ported from Classic/Legacy Opsmgr.</p>
 *
 * <p>This is the entry point for the RI buy analysis.  This class calls the kernel algorithm passing
 * in demand.</p>
 *
 * <p>Class for analyzing a Reserved Instance scenario and generating recommendations.
 * Inputs:
 *  - a scope of analysis (regions, tenancies, platform).
 *  - purchase constraints (specifies what kind of RIs the customer prefers to buy, standard/convertible, term, etc)
 *  - the inventory of RIs that the customer already has
 *  - a list of instance types to consider (eg, t2.small, m4.medium, etc).
 *  - a provider of pricing information for reserved instances (eg RI costs $2/hr vs on-demand @ $3/hr)
 *  - a provider of historical demand (eg, number of VMs using a particular instance type over time)
 * Computation:
 *  For each subset of the specified scope where RIs have the same applicability (eg each region, each platform, etc),
 *  call the RecommendationKernelAlgorithm to determine the number of RIs that the customer should have. Compare with
 *  the existing inventory and generate actions to be taken (eg, buy RIs) to achieve that number.
 * Output:
 *  An AnalysisResult describing the analysis done and a list of recommendations,
 *  of actions to be taken (eg, buy some RIs, convert some others, sell some, etc).
 *<\p>
 *
 * <p>NOTE: The current implementation is only capable of generating Buy recommendations.<\p>
 */
@ThreadSafe
public class ReservedInstanceAnalyzer {

    private static final Logger logger = LogManager.getLogger();

    private final BuyReservedInstanceStore buyRiStore;

    private final SettingServiceBlockingStub settingsServiceClient;

    private final ReservedInstanceActionsSender actionsSender;

    private final RIBuyDemandCalculatorFactory demandCalculatorFactory;

    /**
     * the minimum historical data points required for RI buy.
     */
    private final int riMinimumDataPoints;


    private final ActionContextRIBuyStore actionContextRIBuyStore;

    /*
     * Needed for optimize cloud plan to have the initial topology to start from.
     */
    private final long realtimeTopologyContextId;

    private final RIBuyAnalysisContextProvider analysisContextProvider;

    /*
    * Needed to create the RIBuyRateProvider.
     */
    private final PriceTableStore priceTableStore;
    private final BusinessAccountPriceTableKeyStore baPriceTableStore;

    /**
     * Construct an analyzer.
     *
     * @param settingsServiceClient Setting Services client for fetching settings.
     * @param priceTableStore price table store
     * @param baPriceTableStore Business Account oids to their respective price table key store
     * @param analysisContextProvider Provides unique analysis contexts based on the analysis scope.
     * @param demandCalculatorFactory A factory for demand calculators, used to merge recorded demand
    *                                with RI inventory, in order to calculate uncovered demand for analysis.
     * @param actionsSender used to broadcast the actions.
     * @param buyRiStore Place to store all the buy RIs suggested by this algorithm
     * @param actionContextRIBuyStore the class to perform database operation on the cost.action_context_ri_buy
     * @param realtimeTopologyContextId realtime topology context id
     * @param riMinimumDataPoints RI buy hour data points value range from 1 to 168 inclusive
     */
    public ReservedInstanceAnalyzer(@Nonnull SettingServiceBlockingStub settingsServiceClient,
                                    @Nonnull PriceTableStore priceTableStore,
                                    @Nonnull BusinessAccountPriceTableKeyStore baPriceTableStore,
                                    @Nonnull RIBuyAnalysisContextProvider analysisContextProvider,
                                    @Nonnull RIBuyDemandCalculatorFactory demandCalculatorFactory,
                                    @Nonnull ReservedInstanceActionsSender actionsSender,
                                    @Nonnull BuyReservedInstanceStore buyRiStore,
                                    @Nonnull ActionContextRIBuyStore actionContextRIBuyStore,
                                    final long realtimeTopologyContextId,
                                    final int riMinimumDataPoints) {
        this.settingsServiceClient = Objects.requireNonNull(settingsServiceClient);
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.baPriceTableStore = Objects.requireNonNull(baPriceTableStore);
        this.analysisContextProvider = Objects.requireNonNull(analysisContextProvider);
        this.demandCalculatorFactory = Objects.requireNonNull(demandCalculatorFactory);
        this.actionsSender = Objects.requireNonNull(actionsSender);
        this.buyRiStore = Objects.requireNonNull(buyRiStore);
        this.actionContextRIBuyStore = Objects.requireNonNull(actionContextRIBuyStore);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.riMinimumDataPoints = riMinimumDataPoints;
    }

    @VisibleForTesting
    protected ReservedInstanceAnalyzer() {
        this.settingsServiceClient = null;
        this.priceTableStore = null;
        this.baPriceTableStore = null;
        this.actionsSender = null;
        this.buyRiStore = null;
        this.actionContextRIBuyStore = null;
        this.demandCalculatorFactory = null;
        this.realtimeTopologyContextId = 1L;
        this.analysisContextProvider = null;
        this.riMinimumDataPoints = 1;
    }

    /**
     * Run the RI buy algorithm and send the RI buy actions to the action orchestrator.
     *
     * @param topologyContextId  this may be the plan id or the realtime topology context ID.
     * @param cloudTopology Cloud Topology.
     * @param scope analysis scope
     * @param historicalDemandDataType the type of demand data: allocated or consumption.
     * @throws CommunicationException Exception thrown on errors occurred during communications.
     * @throws InterruptedException Thrown when a thread is waiting, sleeping, or otherwise occupied,
     *                              and the thread is interrupted, either before or during the activity.
     */
    public void runRIAnalysisAndSendActions(final long topologyContextId,
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull ReservedInstanceAnalysisScope scope,
            @Nonnull ReservedInstanceHistoricalDemandDataType historicalDemandDataType)
                throws CommunicationException, InterruptedException {
        ActionPlan actionPlan;
        // If the analysis is for real time delete all entries for real time from
        // action_context_ri_buy table.
        if (topologyContextId == realtimeTopologyContextId) {
            actionContextRIBuyStore.deleteRIBuyContextData(realtimeTopologyContextId);
        }
        ReservedInstanceAnalysisResult result = analyze(topologyContextId, cloudTopology,
                scope, historicalDemandDataType);
        if (result == null) {
            // when result is null, it may be that no need to buy any ri
            // so we create a dummy action plan and send to action orchestrator
            // once action orchestrator receives buy RI action plan, it will
            // notify plan orchestrator it status
            actionPlan = createEmptyActionPlan(topologyContextId);
        } else {
            result.persistResults();
            actionPlan = result.createActionPlan();
        }
        actionsSender.notifyActionsRecommended(actionPlan);
    }

    /**
     * Create a dummy action plan and send to action orchestrator.
     *
     * @param topologyContextId Topology context ID.
     *
     * @return Empty action plan.
     */
    public ActionPlan createEmptyActionPlan(long topologyContextId) {
        return ActionPlan.newBuilder()
                .setId(IdentityGenerator.next())
                .setInfo(ActionPlanInfo.newBuilder()
                        .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                                .setTopologyContextId(topologyContextId)))
                .build();
    }

    /**
     * Clears the realtime actions from the {@link BuyReservedInstanceStore}, {@link ActionContextRIBuyStore},
     * and action-orchestrator (by sending an empty action plan).
     */
    public void clearRealtimeRIBuyActions() {
        try {
            buyRiStore.updateBuyReservedInstances(Collections.emptySet(), realtimeTopologyContextId);
            actionContextRIBuyStore.deleteRIBuyContextData(realtimeTopologyContextId);
            actionsSender.notifyActionsRecommended(createEmptyActionPlan(realtimeTopologyContextId));
        } catch (Exception e) {
            logger.error("Error clearing the realtime RI buy actions", e);
        }
    }

    /**
     * Run the analysis and produce recommendations.
     *
     * @param topologyContextId the topology used to analyze Buy RI.
     * @param cloudTopology Cloud Topology.
     * @param scope describes the scope of the analysis including regions, platforms, etc. under consideration.
     * @param historicalDemandDataType type of data used in RI Buy Analysis -- ALLOCATION or CONSUMPTION based.
     *
     * @return the resulting recommendations plus contextual information, or null
     *         if there was an error. A successful analysis always returns an
     *         AnalysisResult, but it may not contain any recommendations if there
     *         isn't anything to recommend (eg, if the customer has already purchased
     *         adequate RI coverage).
     */
    @Nullable
    public ReservedInstanceAnalysisResult analyze(final long topologyContextId,
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull ReservedInstanceAnalysisScope scope,
            @Nonnull ReservedInstanceHistoricalDemandDataType historicalDemandDataType) {
        Objects.requireNonNull(scope);
        Objects.requireNonNull(historicalDemandDataType);

        final Date analysisStartTime = new Date();
        final Stopwatch overallTime = Stopwatch.createStarted();
        try {
            // Describes what kind of RIs can be considered for purchase. The purchase
            // constraints are retrieved as mapping from the service provider display name.
            final Map<String, ReservedInstancePurchaseConstraints> purchaseConstraints =
                    getPurchaseConstraints(scope);

            // Find the historical demand by context and create analysis clusters.
            final Stopwatch stopWatch = Stopwatch.createStarted();
            final RIBuyAnalysisContextInfo analysisContextInfo = analysisContextProvider.computeAnalysisContexts(
                    scope, purchaseConstraints, cloudTopology);
            if (analysisContextInfo.regionalContexts().isEmpty()) {
                logger.info("Something went wrong: discovered 0 Analyzer clusters, time: {} ms",
                    stopWatch.elapsed(TimeUnit.MILLISECONDS));
            } else {
                logger.info("discovered {} Analyzer clusters, time: {} ms",
                        analysisContextInfo.regionalContexts().size(),
                        stopWatch.elapsed(TimeUnit.MILLISECONDS));


                // A count of the number of separate contexts that were analyzed -- separate
                // combinations of region, tenancy, platform, and instance type or family
                // (depending on whether instance size flexible rules applied).
                Integer clustersAnalyzed = new Integer(0);

                // compute the recommendations
                List<ReservedInstanceAnalysisRecommendation> recommendations = computeRecommendations(
                        analysisContextInfo,
                        clustersAnalyzed,
                        historicalDemandDataType);
                logRecommendations(recommendations);
                removeBuyZeroRecommendations(recommendations);
                ReservedInstanceAnalysisResult result =
                        new ReservedInstanceAnalysisResult(scope, purchaseConstraints, recommendations,
                                topologyContextId, analysisStartTime.getTime(),
                                (new Date()).getTime(), clustersAnalyzed, buyRiStore, actionContextRIBuyStore);

                logger.info("process {} Analyzer clusters for {} recommendations, in time: {} ms",
                        analysisContextInfo.regionalContexts().size(),
                        recommendations.size(),
                        overallTime.elapsed(TimeUnit.MILLISECONDS));
                return result;
            }
        } catch (Exception e) {
            logger.error("Buy RI analysis failed.", e);
        }
        return null;
    }

    /**
     * Remove from the list Recommendations which buy 0 Reserved Instances.
     *
     * @param recommendations The list from which to possibly remove entries
     */
    private void removeBuyZeroRecommendations(List<ReservedInstanceAnalysisRecommendation> recommendations) {
        recommendations.removeIf(recommendation -> recommendation.getCount() == 0);
    }

    /**
     * Log at info level all the recommendations, including those that do not buy anything.
     *
     * @param recommendations The list of Recommendations to log with an appropriate header
     */
    private void logRecommendations(List<ReservedInstanceAnalysisRecommendation> recommendations) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        ps.println(ReservedInstanceAnalysisRecommendation.getCSVHeader());

        for (ReservedInstanceAnalysisRecommendation recommendation : recommendations) {
            ps.println(recommendation.toCSVString());
        }

        String csv = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        ps.close();
        // start the CSV on a new line, so it is easy to extract into Excel
        logger.info("AnalysisResults{}{}", System.lineSeparator(), csv);
    }

    /**
     * Given the purchase constraints the contexts clustered by region, return a list of
     * recommendations of RIs to buy.
     *
     * @param analysisContextInfo The analysis context info.
     * @param clustersAnalyzed keep count of how many clusters were analyzed
     * @param demandDataType what type of demand to use in the computation? e.g. allocation or consumption.
     * @return list of RIs to buy
     */
    private List<ReservedInstanceAnalysisRecommendation> computeRecommendations(
            @Nonnull RIBuyAnalysisContextInfo analysisContextInfo,
            @Nonnull Integer clustersAnalyzed,
            @Nonnull ReservedInstanceHistoricalDemandDataType demandDataType) {

        Preconditions.checkNotNull(analysisContextInfo);
        Preconditions.checkNotNull(clustersAnalyzed);
        Preconditions.checkNotNull(demandDataType);

        List<ReservedInstanceAnalysisRecommendation> recommendations = new ArrayList<>();

        final RIBuyDemandCalculator demandCalculator = demandCalculatorFactory.newCalculator(
                analysisContextInfo.regionalRIMatcherCache(),
                demandDataType);
        //get all the BA oids from analysis context
        final Set<Long> primaryAccounts = new HashSet<>();
        for (RIBuyRegionalContext riBuyRegionalContext : analysisContextInfo.regionalContexts()) {
            final Iterator<RIBuyDemandCluster> iterator =
                            riBuyRegionalContext.demandClusters().iterator();
            while (iterator.hasNext()) {
                primaryAccounts.add(iterator.next().accountOid());
            }
        }
        logger.debug("Number of Business Account OIDs extracted from analysis context: {}", primaryAccounts.size());

        final RIBuyRateProvider rateProvider =
                new RIBuyRateProvider(priceTableStore, baPriceTableStore, primaryAccounts);
        // For each cluster
        for (RIBuyRegionalContext regionalContext : analysisContextInfo.regionalContexts()) {
            final RIBuyDemandCalculationInfo demandCalculationInfo =
                    demandCalculator.calculateUncoveredDemand(regionalContext);

            if (demandCalculationInfo.activeHours() < riMinimumDataPoints) {
                logger.debug("{}activeHours={} < riMinimumDataPoints={}, regionalContext={}, continue",
                        regionalContext.analysisTag(),
                        demandCalculationInfo.activeHours(),
                        riMinimumDataPoints, regionalContext.contextTag());
                continue;
            }

            logger.debug("{}Buy RIs for profile={} in {} from cluster={}",
                    regionalContext.analysisTag(),
                    regionalContext.computeTier().getDisplayName(),
                    regionalContext.region().getDisplayName(),
                    regionalContext.contextTag());
            final long primaryAccountOid = demandCalculationInfo.primaryAccountOid();
            // Get the rates for the first instance type we're considering.
            // This will either be the only one, or if instance size flexible
            // this will be the one with the smallest coupon value.
            PricingProviderResult rates = rateProvider.findRates(primaryAccountOid, regionalContext);
            if (rates == null) {
                // something went wrong with the looking up rates.  Error already logged.
                continue;
            }
            clustersAnalyzed++;
            RecommendationKernelAlgorithmResult kernelResult;
            kernelResult = RecommendationKernelAlgorithm.computation(
                    rates.onDemandRate(),
                    rates.reservedInstanceRate(),
                    removeNegativeDataPoints(demandCalculationInfo.aggregateUncoveredDemand()),
                    regionalContext.contextTag(),
                    regionalContext.analysisTag());
            if (kernelResult == null) {
                continue;
            }
            ReservedInstanceAnalysisRecommendation recommendation = generateRecommendation(
                    regionalContext, demandCalculationInfo, kernelResult, rates);
            if (recommendation != null) {
                recommendations.add(recommendation);
                if (recommendation.getCount() > 0) {
                    // Get the graph data.
                    Map<TopologyEntityDTO, float[]> riBuyChartDemand =
                            demandCalculationInfo.uncoveredDemandByComputeTier();
                    float hourlyOnDemandCost =
                            getHourlyOnDemandCost(primaryAccountOid, regionalContext, riBuyChartDemand, rateProvider);
                    recommendation.setEstimatedOnDemandCost(hourlyOnDemandCost *
                            RIBuyRateProvider.HOURS_IN_A_MONTH
                            * 12 * recommendation.getTermInYears());
                    recommendation.setTemplateTypeHourlyDemand(riBuyChartDemand);
                }
            }
        }
        return recommendations;
    }

    /**
     * An ISF RI can cover multiple compute tiers. A non ISF RI covers only a single computer tier.
     * This method based on what type of recommendation (ISF RI or non ISF RI) returns the on demand
     * cost of all compute tiers being covered or just the single compute tier being covered. This
     * method also factors in how much demand each of that compute tier had.
     *
     * @param primaryAccountOid account oid
     * @param regionalContext The regional context.
     * @param templateTypeHourlyDemand mapping from template to demand in coupons.
     * @param rateProvider The rateProvider to look up the on-demand and RI rates.
     * @return the average hourly charges for the weekly demand.
     */
    public float getHourlyOnDemandCost(
            long primaryAccountOid,
            @Nonnull RIBuyRegionalContext regionalContext,
            @Nonnull final Map<TopologyEntityDTO, float[]> templateTypeHourlyDemand,
            @Nonnull final RIBuyRateProvider rateProvider) {

        float totalCostAcrossWorkloads = 0f;
        for (Entry<TopologyEntityDTO, float[]> entry : templateTypeHourlyDemand.entrySet()) {
            /*
             * An ISF RI Buy recommendation can have different compute tiers covered.
             * We want to calculate the on demand cost of each of those different compute tier demand.
             * We construct a new ReservedInstanceRegionalContext using the compute tier of the current
             * template. The reason in doing so is to be able to use the existing
             * rateAndRIProvider:: lookupOnDemandRate.
             */
            final TopologyEntityDTO computerTier = entry.getKey();
            final float onDemandPrice = rateProvider.lookupOnDemandRate(primaryAccountOid, regionalContext, computerTier);
            final int numberOfCoupons = computerTier.getTypeSpecificInfo().getComputeTier().getNumCoupons();
            float weeklyWorkloadDemand = 0f;
            // The demand is in terms of zonal coupons. So inorder to get actual workload demand we
            // divide each demand by the corresponding zonal coupons.
            for (float f : entry.getValue()) {
                weeklyWorkloadDemand += (f / numberOfCoupons);
            }
            totalCostAcrossWorkloads += (weeklyWorkloadDemand * onDemandPrice);
        }
        return (totalCostAcrossWorkloads / RIBuyDemandCalculator.WEEKLY_DEMAND_DATA_SIZE);
    }

    /**
     * Generate recommendation to buy RIs to provide coverage as specified by the kernel result.
     *
     * @param regionalContext the context in which to buy (eg us-east-1 shared-tenancy Linux).
     * @param demandCalculationInfo The uncovered demand calculcatio info, containing the uncovered demand
     *                              broken down by compute tier.
     * @param kernelResult the results from running the kernel algorithm
     * @param pricing provider of rates and RIs.
     * @return a list of recommendations, which may be empty if no actions are necessary.
     */
    @Nullable
    private ReservedInstanceAnalysisRecommendation generateRecommendation(@Nonnull RIBuyRegionalContext regionalContext,
                                                                          @Nonnull RIBuyDemandCalculationInfo demandCalculationInfo,
                                                                          @Nonnull RecommendationKernelAlgorithmResult kernelResult,
                                                                          @Nonnull PricingProviderResult pricing) {
        Objects.requireNonNull(regionalContext);
        Objects.requireNonNull(kernelResult);
        Objects.requireNonNull(pricing);

        final String logTag = kernelResult.getLogTag();
        final String recommendationTag = logTag.replace(": ", "");
        int numberOfRIsToBuy = kernelResult.getNumberOfRIsToBuy();
        // OM-42801: override RI coverage is disabled
        final String actionGoal = "Max Savings";
        float hourlyCostSavings = kernelResult.getHourlyCostSavings().get(numberOfRIsToBuy);
        if (!logExplanation(kernelResult, logTag, regionalContext, hourlyCostSavings, numberOfRIsToBuy)) {
            return null;
        }

        int riPotentialInCoupons = kernelResult.getRiNormalizedCouponsMap().get(numberOfRIsToBuy);
        float riUsedInCoupons = kernelResult.getRiNormalizedCouponsUsedMap().get(numberOfRIsToBuy);
        ReservedInstanceAnalysisRecommendation recommendation =
            new ReservedInstanceAnalysisRecommendation(recommendationTag,
                actionGoal,
                regionalContext,
                demandCalculationInfo.primaryAccountOid(),
                numberOfRIsToBuy,
                pricing,
                hourlyCostSavings,
                kernelResult.getAverageHourlyCouponDemand(),
                kernelResult.getTotalHours(),
                demandCalculationInfo.activeHours(),
                riPotentialInCoupons,
                riUsedInCoupons);
        return recommendation;
    }

    /*
     * If RI hourly cost is 0, then don't log, because logged previously.  Otherwise log.
     */
    private boolean logExplanation(RecommendationKernelAlgorithmResult kernelResult, String logTag,
                                RIBuyRegionalContext regionalContext, float hourlyCostSavings,
                                int numberOfRIsToBuy) {
        boolean result = true;
        float riHourlyCost = kernelResult.getRiHourlyCost();
        if (riHourlyCost <= 0) {
            result = false;
            if (kernelResult.isValid()) {
                logger.error("{} RI with hourly cost={} (less than or equal zero) "
                        + "and hourly cost saving={} in context={}. ",
                    logTag, riHourlyCost, hourlyCostSavings, regionalContext.contextTag());
            }
        } else {
            logger.debug("{}numberOfRIsToBuy {} > 0 in regionalContext={}",
                logTag, numberOfRIsToBuy, regionalContext.contextTag());
        }
        return result;
    }

    /**
     * Given an array of floats, remove the negative values.
     *
     * @param demand data that may contain "-1".
     * @return demand with "-1" removed.
     */
    @Nullable
    protected float[] removeNegativeDataPoints(float[] demand) {
        if (demand == null || demand.length == 0) {
            return null;
        }
        // remove all "-1" from data
        ArrayList<Float> resultList = new ArrayList<>();
        for (int i = 0; i < demand.length; i++) {
            if (demand[i] >= 0) {
                resultList.add(demand[i]);
            }
        }
        if (resultList.size() == 0) {
            return null;
        }

        // convert to float array.
        float[] results = new float[resultList.size()];
        int i = 0;
        for (Float f: resultList) {
            results[i++] = (f != null ? f : 0f);
        }
        return results;
    }

    @VisibleForTesting
    Map<String, ReservedInstancePurchaseConstraints> getPurchaseConstraints(
            ReservedInstanceAnalysisScope scope) throws IllegalArgumentException {
        if (scope.getRiPurchaseProfiles() == null) {
            final Map<String, ReservedInstancePurchaseConstraints> constraints = getPurchaseConstraints();
            logger.info("Using the global purchase constraint settings: {}", constraints);
            return constraints;
        } else {
            Map<String, RIPurchaseProfile> profiles = scope.getRiPurchaseProfiles();
            final Builder<String, ReservedInstancePurchaseConstraints> constraintBuilder =
                    ImmutableMap.builder();
            profiles.forEach((key, profile) -> {
                if (!profile.hasRiType()) {
                    throw new IllegalArgumentException("No ReservedInstanceType is defined" +
                            " for profile " + key + " in ReservedInstanceAnalysisScope");
                }
                ReservedInstanceType type = profile.getRiType();
                final ReservedInstancePurchaseConstraints constraints =
                        new ReservedInstancePurchaseConstraints(type.getOfferingClass(),
                                type.getTermYears(), type.getPaymentOption());

                constraintBuilder.put(key.toUpperCase(), constraints);

            });
            Map<String, ReservedInstancePurchaseConstraints> constraints = constraintBuilder.build();
            logger.info("Using the purchase profiles in analysis scope: {}",
                    constraints);
            return constraints;
        }
    }

    /**
     * Fetch RI Purchase constraints settings from the Settings Service.
     *
     * @return a Map of the service provider OID and the corresponding
     * RI Purchase Settings constraints object.
     */
    @VisibleForTesting
    protected Map<String, ReservedInstancePurchaseConstraints> getPurchaseConstraints() {

        final List<String> awsSettingNames =
                Stream.of(GlobalSettingSpecs.AWSPreferredOfferingClass,
                        GlobalSettingSpecs.AWSPreferredPaymentOption,
                        GlobalSettingSpecs.AWSPreferredTerm)
                        .map(GlobalSettingSpecs::getSettingName)
                        .collect(Collectors.toList());

        final List<String> azureSettingNames =
                Stream.of(GlobalSettingSpecs.AzurePreferredOfferingClass,
                        GlobalSettingSpecs.AzurePreferredPaymentOption,
                        GlobalSettingSpecs.AzurePreferredTerm)
                        .map(GlobalSettingSpecs::getSettingName)
                        .collect(Collectors.toList());


        final Map<String, Setting> settings = new HashMap<>();
        settingsServiceClient.getMultipleGlobalSettings(
                GetMultipleGlobalSettingsRequest.newBuilder()
                        .addAllSettingSpecName(awsSettingNames)
                        .addAllSettingSpecName(azureSettingNames)
                        .build())
                .forEachRemaining(setting -> {
                    settings.put(setting.getSettingSpecName(), setting);
                });

        ReservedInstancePurchaseConstraints awsReservedInstancePurchaseConstraints =
                new ReservedInstancePurchaseConstraints(
                        OfferingClass.valueOf(
                                settings.get(GlobalSettingSpecs.AWSPreferredOfferingClass.getSettingName()).getEnumSettingValue().getValue()),
                        PreferredTerm.valueOf(
                                settings.get(GlobalSettingSpecs.AWSPreferredTerm.getSettingName()).getEnumSettingValue().getValue()).getYears(),
                        PaymentOption.valueOf(
                                settings.get(GlobalSettingSpecs.AWSPreferredPaymentOption.getSettingName()).getEnumSettingValue().getValue()));

        ReservedInstancePurchaseConstraints azureReservedInstancePurchaseConstraints =
                new ReservedInstancePurchaseConstraints(
                        OfferingClass.valueOf(
                                settings.get(GlobalSettingSpecs.AzurePreferredOfferingClass.getSettingName()).getEnumSettingValue().getValue()),
                        PreferredTerm.valueOf(
                                settings.get(GlobalSettingSpecs.AzurePreferredTerm.getSettingName()).getEnumSettingValue().getValue()).getYears(),
                        PaymentOption.valueOf(
                                settings.get(GlobalSettingSpecs.AzurePreferredPaymentOption.getSettingName()).getEnumSettingValue().getValue()));

        return ImmutableMap.<String, ReservedInstancePurchaseConstraints>builder()
                .put(CategoryPathConstants.AWS.toUpperCase(), awsReservedInstancePurchaseConstraints)
                .put(CategoryPathConstants.AZURE.toUpperCase(), azureReservedInstancePurchaseConstraints)
                .build();

    }

    public ReservedInstanceActionsSender getActionsSender() {
        return actionsSender;
    }
}
