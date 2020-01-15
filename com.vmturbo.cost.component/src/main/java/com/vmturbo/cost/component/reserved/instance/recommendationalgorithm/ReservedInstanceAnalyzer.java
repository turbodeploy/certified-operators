package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.commons.lang.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.reservedinstance.recommendationalgorithm.RecommendationKernelAlgorithm;
import com.vmturbo.commons.reservedinstance.recommendationalgorithm.RecommendationKernelAlgorithmResult;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSender;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalyzerRateAndRIs.PricingProviderResult;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    private final RepositoryServiceBlockingStub repositoryClient;

    private final SettingServiceBlockingStub settingsServiceClient;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final ReservedInstanceActionsSender actionsSender;

    /**
     * the minimum historical data points required for RI buy.
     */
    private final int riMinimumDataPoints;


    /*
     * Stores needed for rates and RIs.
     */
    private final ReservedInstanceBoughtStore riBoughtStore;
    private final ReservedInstanceSpecStore riSpecStore;
    private final PriceTableStore priceTableStore;

    // A counter used to generate unique tags for log entries
    private static AtomicInteger logTagCounter = new AtomicInteger();

    private final ActionContextRIBuyStore actionContextRIBuyStore;

    /*
     * Needed for optimize cloud plan to have the initial topology to start from.
     */
    private final long realtimeTopologyContextId;

    /*
     * Facade to access the historical demand.
     * Refactoring code to improve JUnit testing, because this class is highly dependent on the Spring
     * framework's dependency injection, which is difficult to JUnit test.
     *
     * TODO: This object should be passed into the constructor and eliminate the computeTierDemandStatsStore
     * parameter.
     */
    private final ReservedInstanceAnalyzerHistoricalData historicalDemandDataReader;


    private final float preferredCurrentWeight;

   /**
     * Construct an analyzer.
     *
     * @param settingsServiceClient Setting Services client for fetching settings.
     * @param repositoryClient Repository Service client for fetching topology.
     * @param riBoughtStore Provide current RI bought information.
     * @param riSpecStore Provides details of RI Specs.
     * @param priceTableStore Provides price information for RIs and on-demand instances.
     * @param computeTierDemandStatsStore historical demand data store for instances.
     * @param cloudTopologyFactory Cloud topology factory.
     * @param actionsSender used to broadcast the actions.
     * @param buyRiStore Place to store all the buy RIs suggested by this algorithm
     * @param actionContextRIBuyStore the class to perform database operation on the cost.action_context_ri_buy
     * @param realtimeTopologyContextId realtime topology context id
     * @param preferredCurrentWeight The weight of the current value when added to the  historical data.
     * @param riMinimumDataPoints RI buy hour data points value range from 1 to 168 inclusive
     */
    public ReservedInstanceAnalyzer(@Nonnull SettingServiceBlockingStub settingsServiceClient,
                                    @Nonnull RepositoryServiceBlockingStub repositoryClient,
                                    @Nonnull ReservedInstanceBoughtStore riBoughtStore,
                                    @Nonnull ReservedInstanceSpecStore riSpecStore,
                                    @Nonnull PriceTableStore priceTableStore,
                                    @Nonnull ComputeTierDemandStatsStore computeTierDemandStatsStore,
                                    @Nonnull TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                                    @Nonnull ReservedInstanceActionsSender actionsSender,
                                    @Nonnull BuyReservedInstanceStore buyRiStore,
                                    @Nonnull ActionContextRIBuyStore actionContextRIBuyStore,
                                    final long realtimeTopologyContextId,
                                    final float preferredCurrentWeight,
                                    final int riMinimumDataPoints) {
        this.settingsServiceClient = Objects.requireNonNull(settingsServiceClient);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
        this.actionsSender = Objects.requireNonNull(actionsSender);
        this.buyRiStore = Objects.requireNonNull(buyRiStore);
        this.actionContextRIBuyStore = Objects.requireNonNull(actionContextRIBuyStore);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.riMinimumDataPoints = riMinimumDataPoints;

        /*
         * ComputeTierDemandStatsReader
         */
        this.historicalDemandDataReader =
            new ReservedInstanceAnalyzerHistoricalData(Objects.requireNonNull(computeTierDemandStatsStore));

        /*
         * Access to RI and rates stores.
         */
        this.riBoughtStore = Objects.requireNonNull(riBoughtStore);
        this.riSpecStore = Objects.requireNonNull(riSpecStore);
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.preferredCurrentWeight = preferredCurrentWeight;
        logger.debug("ReservedInstanceAnalyzer constructor");
    }

    @VisibleForTesting
    protected ReservedInstanceAnalyzer() {
        this.settingsServiceClient = null;
        this.repositoryClient = null;
        this.cloudTopologyFactory = null;
        this.actionsSender = null;
        this.buyRiStore = null;
        this.actionContextRIBuyStore = null;
        this.realtimeTopologyContextId = 1L;
        this.historicalDemandDataReader = null;
        this.riBoughtStore = null;
        this.riSpecStore = null;
        this.priceTableStore = null;
        this.preferredCurrentWeight = 0.6f;
        this.riMinimumDataPoints = 1;
    }

    /**
     * Run the RI buy algorithm and send the RI buy actions to the action orchestrator.
     *
     * @param topologyContextId  this may be the plan id or the realtime topology context ID.
     * @param scope analysis scope
     * @param historicalDemandDataType the type of demand data: allocated or consumption.
     * @throws CommunicationException Exception thrown on errors occurred during communications.
     * @throws InterruptedException Thrown when a thread is waiting, sleeping, or otherwise occupied,
     *                              and the thread is interrupted, either before or during the activity.
     */
    public void runRIAnalysisAndSendActions(final long topologyContextId,
                @Nonnull ReservedInstanceAnalysisScope scope,
                @Nonnull ReservedInstanceHistoricalDemandDataType historicalDemandDataType)
                                throws CommunicationException, InterruptedException {
        ActionPlan actionPlan;
        // If the analysis is for real time delete all entries for real time from
        // action_context_ri_buy table.
        if (topologyContextId == realtimeTopologyContextId) {
            actionContextRIBuyStore.deleteRIBuyContextData(realtimeTopologyContextId);
        }
        @Nullable ReservedInstanceAnalysisResult result = analyze(topologyContextId, scope,
            historicalDemandDataType);
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
     * @param topologyContextId Topology context ID
     * @return Empty action plan
     */
    private ActionPlan createEmptyActionPlan(long topologyContextId) {
        return ActionPlan.newBuilder()
                .setId(IdentityGenerator.next())
                .setInfo(ActionPlanInfo.newBuilder()
                        .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                                .setTopologyContextId(topologyContextId)))
                .build();
    }

    /**
     * Run the analysis and produce recommendations.
     *
     * @param topologyContextId the topology used to analyze buy ri
     * @param scope describes the scope of the analysis including regions, platforms, etc. under consideration.
     * @param historicalDemandDataType type of data used in RI Buy Analysis -- ALLOCATION or CONSUMPTION based.
     * @return the resulting recommendations plus contextual information, or null
     *         if there was an error. A successful analysis always returns an
     *         AnalysisResult, but it may not contain any recommendations if there
     *         isn't anything to recommend (eg, if the customer has already purchased
     *         adequate RI coverage).
     */
    @Nullable
    public ReservedInstanceAnalysisResult analyze(final long topologyContextId,
            @Nonnull ReservedInstanceAnalysisScope scope,
            @Nonnull ReservedInstanceHistoricalDemandDataType historicalDemandDataType) {
        Objects.requireNonNull(scope);
        Objects.requireNonNull(historicalDemandDataType);

        final Date analysisStartTime = new Date();
        final Stopwatch overallTime = Stopwatch.createStarted();
        try {
            TopologyEntityCloudTopology cloudTopology =
                computeCloudTopology(repositoryClient, realtimeTopologyContextId, scope);
            if (cloudTopology.size() == 0) {
                logger.error("Something went wrong: no cloudTopology entities, time: {} ms");
                return null;
            }
            // Describes what kind of RIs can be considered for purchase
            final ReservedInstancePurchaseConstraints purchaseConstraints =
                    getPurchaseConstraints(scope);

            // Maps family type (eg t2 or m4) to compute tiers in the family.
            // Within each family, compute tiers are sorted by number of coupons.
            // This mapping is needed to find the compute tier type in a family with the least coupons,
            // which the algorithm buys RIs for.
            Map<String, List<TopologyEntityDTO>> computeTierFamilies =
                computeComputeTierFamilies(cloudTopology);

            // Find the historical demand by context and create analysis clusters.
            final Stopwatch stopWatch = Stopwatch.createStarted();
            Map<ReservedInstanceRegionalContext, List<ReservedInstanceZonalContext>> clusters =
                historicalDemandDataReader.computeAnalysisClusters(scope, computeTierFamilies, cloudTopology);
            if (clusters.isEmpty()) {
                logger.info("Something went wrong: discovered 0 Analyzer clusters, time: {} ms",
                    stopWatch.elapsed(TimeUnit.MILLISECONDS));
            } else {
                logger.info("discovered {} Analyzer clusters, time: {} ms",
                            clusters.size(), stopWatch.elapsed(TimeUnit.MILLISECONDS));

                /*
                 * Facade to access the rate and reserved instances.
                 * Must be constructed here to ensure accurate RI and rating information.
                 */
                ReservedInstanceAnalyzerRateAndRIs rateAndRIProvider =
                    new ReservedInstanceAnalyzerRateAndRIs(priceTableStore, riSpecStore, riBoughtStore);

                // A count of the number of separate contexts that were analyzed -- separate
                // combinations of region, tenancy, platform, and instance type or family
                // (depending on whether instance size flexible rules applied).
                Integer clustersAnalyzed = new Integer(0);

                // compute the recommendations
                List<ReservedInstanceAnalysisRecommendation> recommendations =
                        computeRecommendations(scope, purchaseConstraints, clusters, cloudTopology,
                                rateAndRIProvider,  clustersAnalyzed, historicalDemandDataType);
                logRecommendations(recommendations);
                removeBuyZeroRecommendations(recommendations);
                ReservedInstanceAnalysisResult result =
                        new ReservedInstanceAnalysisResult(scope, purchaseConstraints, recommendations,
                                topologyContextId, analysisStartTime.getTime(),
                                (new Date()).getTime(), clustersAnalyzed, buyRiStore, actionContextRIBuyStore);

                logger.info("process {} Analyzer clusters for {} recommendations, in time: {} ms",
                        clusters.size(), recommendations.size(), overallTime.elapsed(TimeUnit.MILLISECONDS));
                return result;
            }
        } catch (Exception e) {
            logger.error("Buy RI analysis failed.", e);
        }
        return null;
    }

    /**
     * Compute the TopologyEntityCloudTopology.
     *
     * @param repositoryClient access to the repository
     * @param topologyContextId  realtime topology context ID.
     * @param scope analysis scope to restrict what repository entities to consider.
     * @return TopologyEntityCloudTopology
     */
    private TopologyEntityCloudTopology computeCloudTopology(RepositoryServiceBlockingStub repositoryClient,
                                                             long topologyContextId,
                                                             ReservedInstanceAnalysisScope scope) {
        // Fetch the latest topology from repository.
        // What if repository is down? Should we keep re-trying until repository is up?
        // TODO: karthikt - fetch just the entities needed instead of getting all the
        // entities in the topology.
        // we need to fetch the realtime topology even for plan because buy RI runs
        // before any plan topology being constructed, if there is a plan topology
        Stream<TopologyEntityDTO> entities = RepositoryDTOUtil.topologyEntityStream(
            repositoryClient.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                    .setTopologyContextId(topologyContextId)
                    .setReturnType(Type.FULL)
                    .setTopologyType(TopologyType.SOURCE)
                    .build()))
            .map(PartialEntity::getFullEntity);
        TopologyEntityCloudTopology cloudTopology =
            cloudTopologyFactory.newCloudTopology(entities);
        return cloudTopology;
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
     * @param scope analysis scope
     * @param purchaseConstraints what RIs to buy
     * @param regionalToZonalContextMap Cluster analysis mapping:
     *                                  from regional context to set of zonal contexts in that region.
     * @param cloudTopology dictionary for cloud entities
     * @param rateAndRIProvider facade for RIs and rates
     * @param clustersAnalyzed keep count of how many clusters were analyzed
     * @param demandDataType what type of demand to use in the computation? e.g. allocation or consumption.
     * @return list of RIs to buy
     */
    private List<ReservedInstanceAnalysisRecommendation> computeRecommendations(
            ReservedInstanceAnalysisScope scope,
            ReservedInstancePurchaseConstraints purchaseConstraints,
            Map<ReservedInstanceRegionalContext, List<ReservedInstanceZonalContext>> regionalToZonalContextMap,
            TopologyEntityCloudTopology cloudTopology,
            @Nonnull ReservedInstanceAnalyzerRateAndRIs rateAndRIProvider,
            @Nonnull Integer clustersAnalyzed,
            ReservedInstanceHistoricalDemandDataType demandDataType) {
        Objects.requireNonNull(rateAndRIProvider);
        Objects.requireNonNull(clustersAnalyzed);

        // OM-42801: disabled override coverage
        if (scope.getOverrideRICoverage()) {
            logger.info("The scope has RI Coverage Override set to {}%."
                            + "This setting is ignored and the maximal savings algorithm is run.",
                    scope.getPreferredCoverage() * 100);
        }
        List<ReservedInstanceAnalysisRecommendation> recommendations = new ArrayList<>();
        // For each cluster
        for (Entry<ReservedInstanceRegionalContext, List<ReservedInstanceZonalContext>> entry :
                regionalToZonalContextMap.entrySet()) {

            // We want to tag all messages related to a context with a key so that we can grep in log
            final String logTag = generateLogTag();
            ReservedInstanceRegionalContext regionalContext = entry.getKey();
            List<ReservedInstanceZonalContext> zonalContexts = entry.getValue();
            // Compute computeTier to buy RIs for.  If compute tier is flexible,
            // it must be the smallest compute tier for this family.
            TopologyEntityDTO buyComputeTier = regionalContext.getComputeTier();

            ReservedInstanceDataProcessor dataProcessor = new ReservedInstanceDataProcessor(logTag,
                historicalDemandDataReader, regionalContext, zonalContexts, scope, demandDataType,
                cloudTopology, rateAndRIProvider, preferredCurrentWeight);
            final float[] riBuyDemand = dataProcessor.getRIBuyDemand();

            if (ArrayUtils.isEmpty(riBuyDemand)) {
                logger.debug("{}no demand for cluster {}", logTag, regionalContext);
                continue;
            }
            int activeHours = countActive(riBuyDemand);
            if (activeHours < riMinimumDataPoints) {
                logger.debug("{}activeHours={} < riMinimumDataPoints={}, regionalContext={}, continue",
                    logTag, activeHours, riMinimumDataPoints, regionalContext);
                continue;
            }

            logger.debug("{}Buy RIs for profile={} in {} from context={}", logTag,
                            buyComputeTier.getDisplayName(), regionalContext.getRegionDisplayName(),
                            regionalContext);
            // Get the rates for the first instance type we're considering.
            // This will either be the only one, or if instance size flexible
            // this will be the one with the smallest coupon value.
            PricingProviderResult rates = rateAndRIProvider.findRates(regionalContext,
                            purchaseConstraints, logTag);
            if (rates == null) {
                // something went wrong with the looking up rates.  Error already logged.
                continue;
            }
            clustersAnalyzed++;
            RecommendationKernelAlgorithmResult kernelResult;
            kernelResult = RecommendationKernelAlgorithm.computation(rates.getOnDemandRate(),
                rates.getReservedInstanceRate(), removeNegativeDataPoints(riBuyDemand), regionalContext.toString(), logTag);
            if (kernelResult == null) {
                continue;
            }
            ReservedInstanceAnalysisRecommendation recommendation = generateRecommendation(scope,
                    regionalContext, purchaseConstraints, kernelResult, rates, rateAndRIProvider,
                activeHours);
            if (recommendation != null) {
                recommendations.add(recommendation);
                if (recommendation.getCount() > 0) {
                    // Get the graph data.
                    Map<TopologyEntityDTO, float[]> riBuyChartDemand = dataProcessor.getRIBuyChartDemand();
                    float hourlyOnDemandCost = getHourlyOnDemandCost(riBuyChartDemand,
                                    regionalContext, rateAndRIProvider, logTag);
                    recommendation.setEstimatedOnDemandCost(hourlyOnDemandCost *
                            ReservedInstanceAnalyzerRateAndRIs.HOURS_IN_A_MONTH
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
     * @param templateTypeHourlyDemand mapping from template to demand in coupons.
     * @param regionalContext Regional Context.
     * @param rateAndRIProvider Facade to access the rate and reserved instances.
     * @param logTag A unique string to identify related messages for a particular RI Buy analysis
     * @return the average hourly charges for the weekly demand.
     */
    public float getHourlyOnDemandCost(final Map<TopologyEntityDTO, float[]> templateTypeHourlyDemand,
                                       ReservedInstanceRegionalContext regionalContext,
                                       ReservedInstanceAnalyzerRateAndRIs rateAndRIProvider,
                                       String logTag) {
        float totalCostAcrossWorkloads = 0f;
        for (Entry<TopologyEntityDTO, float[]> entry : templateTypeHourlyDemand.entrySet()) {
            /*
             * An ISF RI Buy recommendation can have different compute tiers covered.
             * We want to calculate the on demand cost of each of those different compute tier demand.
             * We construct a new ReservedInstanceRegionalContext using the compute tier of the current
             * template. The reason in doing so is to be able to use the existing
             * rateAndRIProvider:: lookupOnDemandRate.
             */
            ReservedInstanceRegionalContext newRegionalContext =
                            new ReservedInstanceRegionalContext(
                                            regionalContext.getMasterAccountId(),
                                            regionalContext.getPlatform(),
                                            regionalContext.getTenancy(),
                                            entry.getKey(),
                                            regionalContext.getRegion());
            float onDemandPrice = rateAndRIProvider.lookupOnDemandRate(newRegionalContext, logTag);
            int numberOfCoupons = entry.getKey().getTypeSpecificInfo().getComputeTier().getNumCoupons();
            float weeklyWorkloadDemand = 0f;
            // The demand is in terms of zonal coupons. So inorder to get actual workload demand we
            // divide each demand by the corresponding zonal coupons.
            for (float f : entry.getValue()) {
                weeklyWorkloadDemand += (f / numberOfCoupons);
            }
            totalCostAcrossWorkloads += (weeklyWorkloadDemand * onDemandPrice);
        }
        return (totalCostAcrossWorkloads / ReservedInstanceDataProcessor.WEEKLY_DEMAND_DATA_SIZE);
    }

    private int countActive(@Nonnull float[] normalizedDemand) {
        int activeHours = 0;
        for (int index = 0; index < normalizedDemand.length; index++) {
            activeHours += normalizedDemand[index] > 0 ? 1 : 0;
        }
        return activeHours;
    }

    /**
     * Generate a unique string in each JRE invocation to use as a log entry tag.
     * Uses atomic integer to make it thread safe.
     *
     * @return Unique String within this invocation of a JRE to use as tag
     */
    private static String generateLogTag() {
        int count = logTagCounter.getAndIncrement();
        return String.format("RILT%04d: ", count);
    }

    /**
     * Generate recommendation to buy RIs to provide coverage as specified by the kernel result.
     *
     * @param scope scope of analysis
     * @param regionalContext the context in which to buy (eg us-east-1 shared-tenancy Linux).
     * @param constraints the constraints under which to purchase RIs.
     * @param kernelResult the results from running the kernel algorithm
     * @param pricing provider of rates and RIs.
     * @param rateAndRIProvider facade to access RIs and rates.
     * @param activeHours how many hours had data?
     * @return a list of recommendations, which may be empty if no actions are necessary.
     */
    @Nullable
    private ReservedInstanceAnalysisRecommendation generateRecommendation(@Nonnull ReservedInstanceAnalysisScope scope,
                                                                          @Nonnull ReservedInstanceRegionalContext regionalContext,
                                                                          @Nonnull ReservedInstancePurchaseConstraints constraints,
                                                                          @Nonnull RecommendationKernelAlgorithmResult kernelResult,
                                                                          @Nonnull PricingProviderResult pricing,
                                                                          @Nonnull ReservedInstanceAnalyzerRateAndRIs rateAndRIProvider,
                                                                          int activeHours) {
        Objects.requireNonNull(scope);
        Objects.requireNonNull(regionalContext);
        Objects.requireNonNull(constraints);
        Objects.requireNonNull(kernelResult);
        Objects.requireNonNull(pricing);
        Objects.requireNonNull(rateAndRIProvider);

        final String logTag = kernelResult.getLogTag();
        final String recommendationTag = logTag.replace(": ", "");
        int numberOfRIsToBuy = kernelResult.getNumberOfRIsToBuy();
        // OM-42801: override RI coverage is disabled
        final String actionGoal = "Max Savings";
        float hourlyCostSavings = kernelResult.getHourlyCostSavings().get(numberOfRIsToBuy);
        if (!logExplanation(kernelResult, logTag, regionalContext, hourlyCostSavings, numberOfRIsToBuy)) {
            return null;
        }

        ReservedInstanceSpec riSpec = rateAndRIProvider.lookupReservedInstanceSpec(regionalContext, constraints);
        if (riSpec == null) {
            // error log message generated in rateAndRIProvider.lookupReservedInstanceSpec
            return null;
        }
        int riPotentialInCoupons = kernelResult.getRiNormalizedCouponsMap().get(numberOfRIsToBuy);
        float riUsedInCoupons = kernelResult.getRiNormalizedCouponsUsedMap().get(numberOfRIsToBuy);
        ReservedInstanceAnalysisRecommendation recommendation =
            new ReservedInstanceAnalysisRecommendation(recommendationTag,
                actionGoal,
                regionalContext,
                constraints,
                ReservedInstanceAnalysisRecommendation.Action.BUY,
                numberOfRIsToBuy,
                pricing,
                hourlyCostSavings,
                kernelResult.getAverageHourlyCouponDemand(),
                kernelResult.getTotalHours(),
                activeHours,
                riPotentialInCoupons,
                riUsedInCoupons,
                riSpec, scope.getTopologyInfo());
        return recommendation;
    }

    /*
     * If RI hourly cost is 0, then don't log, because logged previously.  Otherwise log.
     */
    private boolean logExplanation(RecommendationKernelAlgorithmResult kernelResult, String logTag,
                                ReservedInstanceRegionalContext regionalContext, float hourlyCostSavings,
                                int numberOfRIsToBuy) {
        boolean result = true;
        float riHourlyCost = kernelResult.getRiHourlyCost();
        if (riHourlyCost <= 0) {
            result = false;
            if (kernelResult.isValid()) {
                logger.error("{} RI with hourly cost={} (less than or equal zero) "
                        + "and hourly cost saving={} in cluster={}. ",
                    logTag, riHourlyCost, hourlyCostSavings, regionalContext);
            }
        } else {
            logger.debug("{}numberOfRIsToBuy {} > 0 in regionalContext={}",
                logTag, numberOfRIsToBuy, regionalContext);
        }
        return result;
    }

    /**
     * Group ComputeTiers into families, sorted by coupon value within each family.
     *
     * @param cloudTopology dictionary for topology entities.
     * @return A map with keys being ComputeTier family names (eg, t2 or m4) and the values
     * being a list of the computeTiers in that family, ordered by ascending coupon value.
     */
    @Nonnull
    protected Map<String, List<TopologyEntityDTO>> computeComputeTierFamilies(TopologyEntityCloudTopology cloudTopology) {

        /*
         * If coupon information is absent, we can't do coupon-based analysis.
         *
         * From a perspective of "first, do no harm", it seems better to
         * not issue any recommendation than to recommend buying RIs
         * that may not be needed.
         *
         * A new Turbo release can always fix the coupon mapping in
         * a relatively short time frame, allowing the customer to
         * get accurate recommendations soonish and limiting how long
         * they're wasting money. On the other hand, if the customer buys
         * a RI they don't need, they may be locked into as long as
         * 3 years of waste, or taking a big loss selling it.
         *
         * So, if we don't have full information, skip analysis for this size
         * entirely.
         */
        Map<String, List<TopologyEntityDTO>> result =
                cloudTopology.getEntities().values()
                .stream()
                .filter(entityDTO -> entityDTO.getEntityType() == EntityType.COMPUTE_TIER_VALUE)
                .filter(TopologyEntityDTO::hasTypeSpecificInfo)
                .filter(entityDTO -> entityDTO.getTypeSpecificInfo().hasComputeTier())
                .filter(entityDTO -> entityDTO.getTypeSpecificInfo().getComputeTier().getNumCoupons() > 0)
                // Within each family, sort in coupon value order
                // sort in ascending order of the number of coupons.
                .sorted(Comparator.comparingInt(entityDTO -> entityDTO.getTypeSpecificInfo().getComputeTier().getNumCoupons()))
                .collect(Collectors.groupingBy(entityDTO ->
                        entityDTO.getTypeSpecificInfo().getComputeTier().getFamily()));
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

    private ReservedInstancePurchaseConstraints getPurchaseConstraints(
                    ReservedInstanceAnalysisScope scope) throws IllegalArgumentException {
        if (scope.getRiPurchaseProfile() == null) {
            return getPurchaseConstraints();
        } else {
            RIPurchaseProfile profile = scope.getRiPurchaseProfile();
            if (!profile.hasRiType()) {
                throw new IllegalArgumentException(
                                "No ReservedInstanceType is defined in ReservedInstanceAnalysisScope");
            }
            ReservedInstanceType type = profile.getRiType();
            return new ReservedInstancePurchaseConstraints(type.getOfferingClass(),
                            type.getTermYears(), type.getPaymentOption());

        }
    }

    /**
     * Fetch RI Purchase constraints settings from the Settings Service.
     *
     * @return RI Purchase Settings constraints object.
     */
    private ReservedInstancePurchaseConstraints getPurchaseConstraints() {
        List<String> settingNames =
                Arrays.asList(GlobalSettingSpecs.AWSPreferredOfferingClass,
                        GlobalSettingSpecs.AWSPreferredPaymentOption,
                        GlobalSettingSpecs.AWSPreferredTerm)
                    .stream()
                    .map(GlobalSettingSpecs::getSettingName)
                    .collect(Collectors.toList());

        Map<String, Setting> settings = new HashMap<>();
        settingsServiceClient.getMultipleGlobalSettings(
                GetMultipleGlobalSettingsRequest.newBuilder().build().newBuilder()
                        .addAllSettingSpecName(settingNames)
                        .build())
        .forEachRemaining( setting -> {
            settings.put(setting.getSettingSpecName(), setting);
        });

        ReservedInstancePurchaseConstraints reservedInstancePurchaseConstraints =
                new ReservedInstancePurchaseConstraints(
                        OfferingClass.valueOf(
                                settings.get(GlobalSettingSpecs.AWSPreferredOfferingClass.getSettingName()).getEnumSettingValue().getValue()),
                        PreferredTerm.valueOf(
                                settings.get(GlobalSettingSpecs.AWSPreferredTerm.getSettingName()).getEnumSettingValue().getValue()).getYears(),
                        PaymentOption.valueOf(
                                settings.get(GlobalSettingSpecs.AWSPreferredPaymentOption.getSettingName()).getEnumSettingValue().getValue()));

        return reservedInstancePurchaseConstraints;

    }
}
