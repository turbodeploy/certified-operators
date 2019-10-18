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

import com.google.common.base.Stopwatch;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
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
     * Refatoring code to improve JUnit testing, because this class is highly dependent on the Spring
     * framework's dependency injection, which is difficult to JUnit test.
     *
     * TODO: This object should be passed into the constructor and eliminate the computeTierDemandStatsStore
     * parameter.
     */
    private final ReservedInstanceAnalyzerHistoricalData historicalDemandDataReader;

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
                                    final long realtimeTopologyContextId) {
        this.settingsServiceClient = Objects.requireNonNull(settingsServiceClient);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
        this.actionsSender = Objects.requireNonNull(actionsSender);
        this.buyRiStore = Objects.requireNonNull(buyRiStore);
        this.actionContextRIBuyStore = Objects.requireNonNull(actionContextRIBuyStore);
        this.realtimeTopologyContextId = realtimeTopologyContextId;

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

        logger.debug("ReservedInstanceAnalyzer constructor");
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
       if (historicalDemandDataReader.containsDataOverWeek()) {
            @Nullable ReservedInstanceAnalysisResult result = analyze(topologyContextId, scope,
                historicalDemandDataType);
            ActionPlan actionPlan;
            if (result == null) {
                // when result is null, it may be that no need to buy any ri
                // so we create a dummy action plan and send to action orchestrator
                // once action orchestrator receives buy RI ation plan, it will
                // notify plan orchestrator it status
                actionPlan = ActionPlan.newBuilder()
                    .setId(IdentityGenerator.next())
                    .setInfo(ActionPlanInfo.newBuilder()
                        .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                            .setTopologyContextId(topologyContextId)))
                    .build();
            } else {
                result.persistResults();
                actionPlan = result.createActionPlan();
            }
            actionsSender.notifyActionsRecommended(actionPlan);
        } else {
            logger.info("There is less than one week of data available, waiting for more data to" +
                " trigger buy RI analysis.");

        }
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
        logger.info("AnalysisResults {}",  csv);
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

            Map<ReservedInstanceZonalContext, float[]> dBDemand =
                getDemandFromDB(scope, zonalContexts, demandDataType);

            /*
             * We are doing a deep copy here because the original map gets modified in subsequent
             * steps (through normalization). dBDemandDeepCopy is going to be used for RI
             * Buy graph where we want to show values from the DB (in terms of NFU's) rather than
             * the normalized values.
             */
            final Map<ReservedInstanceZonalContext, float[]> dBDemandDeepCopy = dBDemand.entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> Arrays.copyOf(e.getValue(),
                            e.getValue().length)));

            // get the historical demand for each RI context
            Map<ReservedInstanceZonalContext, float[]> riContextDemand =
                    normalizeDemand(dBDemand, regionalContext, cloudTopology, logTag);
            // apply zonal RIs
            float[] normalizedDemand =
                    applyZonalRIsToCluster(riContextDemand, regionalContext, rateAndRIProvider);
            // apply regional RIs and return the normalized array of demands
            normalizedDemand =
                    applyRegionalRIs(normalizedDemand, regionalContext, rateAndRIProvider,
                            cloudTopology.getEntities());
            if (ArrayUtils.isEmpty(normalizedDemand)) {
                logger.debug("{}no demand for cluster {}", logTag, regionalContext);
                continue;
            }
            logger.debug("{}Buy RIs for profile={} in context={}",
                    logTag, buyComputeTier.getDisplayName(), regionalContext);
            // Get the rates for the first instance type we're considering.
            // This will either be the only one, or if instance size flexible
            // this will be the one with the smallest coupon value.
            PricingProviderResult rates = rateAndRIProvider.findRates(regionalContext, purchaseConstraints);
            if (rates == null) {
                // something went wrong with the looking up rates.
                logger.warn("analyze() rates are null for regionContext={} and constraints={}",
                    regionalContext, purchaseConstraints);
                continue;
            }
            clustersAnalyzed++;
            RecommendationKernelAlgorithmResult kernelResult;
            kernelResult = RecommendationKernelAlgorithm.computation(rates.getOnDemandRate(),
                rates.getReservedInstanceRate(), normalizedDemand, regionalContext.toString(), logTag);
            if (kernelResult == null) {
                continue;
            }
            int activeHours = countActive(normalizedDemand, logTag);
            ReservedInstanceAnalysisRecommendation recommendation = generateRecommendation(scope,
                    regionalContext, purchaseConstraints, kernelResult, rates, rateAndRIProvider,
                activeHours);
            if (recommendation != null) {
                recommendations.add(recommendation);
                if (recommendation.getCount() > 0) {
                    final Map<TopologyEntityDTO, Float[]> templateTypeHourlyDemand = new HashMap<>();
                    for (Entry<ReservedInstanceZonalContext, float[]> e : dBDemandDeepCopy.entrySet()) {
                        final ReservedInstanceZonalContext zonalContext = e.getKey();
                        final float[] currentDemandInWorkLoad = e.getValue();

                        final TopologyEntityDTO computeTier = zonalContext.getComputeTier();
                        final int numberOfCoupons = computeTier.getTypeSpecificInfo()
                                .getComputeTier().getNumCoupons();

                        Float[] demandInCoupons = templateTypeHourlyDemand.get(computeTier);
                        if (ArrayUtils.isEmpty(demandInCoupons)) {
                            demandInCoupons = new Float[currentDemandInWorkLoad.length];
                            Arrays.fill(demandInCoupons, 0f);
                        }

                        // Add the current demand to the existing demand for the same template type.
                        // Also convert the '-1' demand to 0 for the context.
                        for (int i = 0; i < demandInCoupons.length; i++) {
                            demandInCoupons[i] = demandInCoupons[i] + Math.max(0, (currentDemandInWorkLoad[i]
                                    * numberOfCoupons));
                        }
                        templateTypeHourlyDemand.put(computeTier, demandInCoupons);
                    }
                    recommendation.setTemplateTypeHourlyDemand(templateTypeHourlyDemand);
                }
            }
        }
        return recommendations;
    }

    private int countActive(@Nonnull float[] normalizedDemand, @Nonnull String logTag) {
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
     * Apply zonal RI with the demand. The instance type in the context may be different than the
     * instance type in the cluster if instance size flexible. Then we combine the results with
     * combineDemand. We Also remove the negative unpopulated data points before return the array
     * of demands.
     *
     * @param zonalContextDemands a map of from zonal context to demand.
     * @param regionalContext       the scope of analysis, regionalContext.
     * @param priceAndRIProvider   provider of rates and RIs.
     * @return a normalized array of demands.
     */
    @Nullable
    float[] applyZonalRIsToCluster(@Nonnull Map<ReservedInstanceZonalContext, float[]> zonalContextDemands,
                                   @Nonnull ReservedInstanceRegionalContext regionalContext,
                                   @Nonnull ReservedInstanceAnalyzerRateAndRIs priceAndRIProvider) {

        List<float[]> demands = new ArrayList<>();
        for (ReservedInstanceZonalContext zonalContext : zonalContextDemands.keySet()) {
            float[] demand = applyZonalRIs(zonalContextDemands.get(zonalContext),
                    zonalContext, regionalContext.getComputeTier(), priceAndRIProvider);
            if (demand != null) {
                demands.add(demand);
            }
        }
        float[] combinedDemand = combineDemand(demands);
        return removeNegativeDataPoints(combinedDemand);
    }

    /**
     * Return demand adjusted by coupons from applicable zonal RIs.
     * For zonal RIs, instance size flexible is not allowed.
     * Multiple zonal RIs can apply to a single context.  Only one context can apply to a zonal RI.
     *
     * @param normalizedDemand historical demand, in normalized coupons.
     * @param zonalContext the context that is looking for zonal RIs.
     * @param buyComputeTier the compute tier buying RIs for and whose number of coupons normalizes RI coupons.
     * @param priceAndRIProvider   provider of rates and RIs.
     * @return normalizedDemand adjust for zonal RIs.
     */
    float[] applyZonalRIs(@Nullable float[] normalizedDemand,
                          @Nonnull ReservedInstanceZonalContext zonalContext,
                          @Nonnull TopologyEntityDTO buyComputeTier,
                          @Nonnull ReservedInstanceAnalyzerRateAndRIs priceAndRIProvider) {
        List<ReservedInstanceBoughtInfo> risBought =
            priceAndRIProvider.lookupReservedInstanceBoughtInfos(zonalContext.getMasterAccountId(),
                zonalContext.getAvailabilityZoneId());

        if (CollectionUtils.isEmpty(risBought)) {
            logger.debug("No bought Zonal RI present to be subtracted in cluster {}", zonalContext);
            return normalizedDemand;
        }

        int normalizedCoupons = risBought.stream()
                .mapToInt(riBoughtInfo -> {
                    ReservedInstanceSpec riSpec =
                        priceAndRIProvider.lookupReservedInstanceSpecWithId(riBoughtInfo.getReservedInstanceSpec());
                    if (riSpec == null) {
                        logger.warn("RISpec is null. Skipping RI: {}", riBoughtInfo);
                        //skip this RI.
                        return 0;
                    }
                    ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
                    if (riSpecInfo.getTenancy() == zonalContext.getTenancy()
                            && riSpecInfo.getOs() == zonalContext.getPlatform()
                            && riSpecInfo.getTierId() == zonalContext.getComputeTier().getOid()) {
                        return (riBoughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons() /
                            buyComputeTier.getTypeSpecificInfo().getComputeTier().getNumCoupons());
                    }
                    return 0;
                })
                .sum();

        if (normalizedCoupons == 0) {
            logger.debug("no zonal RIs applicable in context {}", zonalContext);
            return normalizedDemand;
        }
        return subtractCouponsFromDemand(normalizedDemand, normalizedCoupons);
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
            float[] demand = historicalDemandDataReader.getDemand(scope, context, demandDataType);
            demands.put(context, demand);
        }
        return demands;
    }

    /**
     * Return demand adjusted by regional RIs.
     *
     * @param normalizedDemand historical demand, in coupons normalized by regional context's compute tier.
     * @param regionalContext scope of analysis cluster.
     * @param rateAndRIProvider provider of rates and RIs.
     * @param cloudEntities dictionary for cloud entities.
     * @return normalized demand adjusted by regional RIs
     */
    float[] applyRegionalRIs(float[] normalizedDemand,
                             @Nonnull ReservedInstanceRegionalContext regionalContext,
                             @Nonnull ReservedInstanceAnalyzerRateAndRIs rateAndRIProvider,
                             @Nonnull Map<Long, TopologyEntityDTO> cloudEntities) {
        Objects.requireNonNull(regionalContext);
        Objects.requireNonNull(rateAndRIProvider);
        Objects.requireNonNull(cloudEntities);

        List<ReservedInstanceBoughtInfo> risBought =
            rateAndRIProvider.lookupReservedInstancesBoughtInfos(regionalContext);

        if (CollectionUtils.isEmpty(risBought)) {
            logger.debug("No bought Regional RI present to be subtracted in cluster {}", regionalContext);
            return normalizedDemand;
        }

        int buyComputeTierCoupons = regionalContext.getComputeTier().getTypeSpecificInfo().getComputeTier().getNumCoupons();
        float normalizedCoupons = 0;
        for (ReservedInstanceBoughtInfo boughtInfo: risBought) {
            ReservedInstanceSpec riSpec =
                rateAndRIProvider.lookupReservedInstanceSpecWithId(boughtInfo.getReservedInstanceSpec());
            if (riSpec == null || !boughtInfo.hasBusinessAccountId()) {
                logger.warn("applyRegionalRIs Skipping RI: {}", boughtInfo);
                continue;
            }
            ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
            if (riSpecInfo.getRegionId() == regionalContext.getRegionId()
                // might have to look up master account for boughtInfo
                && boughtInfo.getBusinessAccountId() == regionalContext.getMasterAccountId()
                && riSpecInfo.getTenancy() == regionalContext.getTenancy()
                && riSpecInfo.getOs() == regionalContext.getPlatform()
                && (regionalContext.isInstanceSizeFlexible() ?
                (regionalContext.getComputeTier().getTypeSpecificInfo().getComputeTier().getFamily() ==
                    cloudEntities.get(riSpecInfo.getTierId()).getTypeSpecificInfo().getComputeTier().getFamily())
                : riSpecInfo.getTierId() == regionalContext.getComputeTier().getOid())
                && (regionalContext.getComputeTier().getTypeSpecificInfo().getComputeTier().getNumCoupons() > 0)) {
                normalizedCoupons +=
                    boughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons() /
                        (float)buyComputeTierCoupons;
            }
        }
        if (normalizedCoupons == 0) {
            logger.debug("No regional RIs applicable in cluster {}", regionalContext);
            return normalizedDemand;
        }

        return subtractCouponsFromDemand(normalizedDemand, normalizedCoupons);
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

        float riHourlyCost = kernelResult.getRiHourlyCost();
        if (riHourlyCost <= 0) {
            logger.error("{}Reserved Instance regionalContext={} has RI with hourly RI cost={} "
                            + "(less than or equal zero) and hourly cost saving {}. ",
                            logTag, regionalContext, riHourlyCost, hourlyCostSavings);
        }

        logger.debug("{}numberOfRIsToBuy {} > 0  for computeTierId={} in regionalContext={}",
            logTag, numberOfRIsToBuy, regionalContext.getComputeTier().getOid(), regionalContext);
        ReservedInstanceSpec riSpec = rateAndRIProvider.lookupReservedInstanceSpec(regionalContext, constraints);
        if (riSpec == null) {
            logger.error("Could not find riSpec for {}", regionalContext);
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
                pricing.getOnDemandRate(),
                pricing.getReservedInstanceRate(),
                hourlyCostSavings,
                kernelResult.getAverageHourlyCouponDemand(),
                kernelResult.getTotalHours(),
                activeHours,
                riPotentialInCoupons,
                riUsedInCoupons,
                riSpec);

        return recommendation;
    }

    /**
     * Normalize the demand for an analysis cluster.  This is required for instance size flexible
     * where demand from different size templates are analyzed together.
     * The demand for each zonal context is normalized by the number of coupons for the regional
     * cluster's compute tier.
     * Note dbDemand, which was obtained from the database, may contain "-1" values.
     * Want the number of coupons relative to compute tier that RIs are bought for, buyComputeTier.
     * For example, "c4" family, minimum size if 'large', which is 8 coupons, want the
     * number of coupons normalized by 8.
     * Constraint:
     *   zonalContext.getInstanceType().getNumberOfCoupons() >= regionalContext.getInstanceType().getNumberOfCoupons()
     *
     * @param dBDemand a map from zonal context to demand.
     * @param regionalContext  the scope of analysis cluster, a regional context.
     * @param cloudTopology topology entity dictionary for cloud entities.
     * @param logTag tag for logging messages.
     * @return a map from zonal context to array of demand.
     */
    Map<ReservedInstanceZonalContext, float[]> normalizeDemand(Map<ReservedInstanceZonalContext, float[]> dBDemand,
                                                               ReservedInstanceRegionalContext regionalContext,
                                                               TopologyEntityCloudTopology cloudTopology,
                                                               String logTag) {

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
     * Aggregates up multiple demand histories. For example, to add together zone
     * demand to get regional demand or to add instance types across a region.
     *
     * <p>All arrays must be of the same length.
     * Because the database can return an array with values of -1, to indicate the data has never been populated,
     * this routine does not remove -1 data values.
     * </p>
     *
     * @param demandHistories a list of one or more demand arrays, which must all be the same length.
     *                       Can be null or empty if there is no demand.  A demand array may contain "-1" values.
     * @return the combined demand.
     */
    @Nullable
    protected float[] combineDemand(@Nullable List<float[]> demandHistories) {
        if (CollectionUtils.isEmpty(demandHistories)) {
            return null;
        }

        // assumption all arrays are of the same length.
        int length = demandHistories.get(0).length;

        for (float[] demandHistory : demandHistories) {
            assert length == demandHistory.length;
        }

        // combined result may contain "-1" values to ensure proper combining.
        float[] combinedResult = new float[length];
        Arrays.fill(combinedResult, -1f);  // initialize to unpopulated.

        boolean foundData = false;
        for (float[] demandHistory : demandHistories) {
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
     * Given an array of floats, remove the negative values.
     *
     * @param demand data that may contain "-1".
     * @return demand with "-1" removed.
     */
    @Nullable
    protected float[] removeNegativeDataPoints(float[] demand) {
        if (ArrayUtils.isEmpty(demand)) {
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

    /**
     * Given an array of floats, sum the values. Ignoring any negative entries.
     *
     * @param demand data that may contain "-1".
     * @return demand with "-1" removed.
     */
    @Nullable
    protected float sumDataPoints(float[] demand) {
        float result = 0f;
        if (ArrayUtils.isEmpty(demand)) {
            return result;
        }
        for (int i = 0; i < demand.length; i++) {
            if (demand[i] >= 0) {
                result = demand[i] + result;
            }
        }
        return result;
    }


    @Nullable
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
     * Compute uncovered demand: given a demand history, subtract out the demand that
     * is covered by existing RIs.
     * Two cases:
     * 1) A data point in the demand history is populated
     * and has a value > 0, then don't allow value to go negative.
     * 2) A data point in the demand history is not populated and has a value == "-1", keep its value as "-1".
     *
     * @param demand Historical demand, on coupons, over time.
     * @param coupons The number of coupons to be subtracted from the demand.
     *                Guaranteed to be greater than 0.
     * @return the adjusted historical demand.  The length is the same as the input demand array.
     */
    @Nullable
    protected float[] subtractCouponsFromDemand(@Nullable float[] demand, float coupons) {
        if (ArrayUtils.isEmpty(demand)) {
            return null;
        }
        if (coupons == 0) {
            return demand;
        }
        float[] adjustedDemand = new float[demand.length];

        boolean containsANonZeroValue = false;
        for (int i = 0; i < demand.length; i++) {
            // if un-populated data, i.e. "-1", then keep
            if (demand[i] > 0) {
                // Do not allow populated data to go negative.
                containsANonZeroValue = true;
                adjustedDemand[i] = Math.max(0.0f, demand[i] - coupons);
            } else {
                adjustedDemand[i] = demand[i];  // this could be 0 or "-1"
            }
        }

        if (containsANonZeroValue == false) {
            return null;
        }
        return adjustedDemand;
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

    private ReservedInstancePurchaseConstraints getPurchaseConstraints(ReservedInstanceAnalysisScope scope)
            throws IllegalArgumentException {
        if (scope.getRiPurchaseProfile() == null) {
            return getPurchaseConstraints();
        } else {
            RIPurchaseProfile profile = scope.getRiPurchaseProfile();
            if (!profile.hasRiType()) {
                throw new IllegalArgumentException("No ReservedInstanceType is defined in ReservedInstanceAnalysisScope");
            }
            ReservedInstanceType type = profile.getRiType();
            return new ReservedInstancePurchaseConstraints(type.getOfferingClass(), type.getTermYears(),
                                                           type.getPaymentOption());

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
