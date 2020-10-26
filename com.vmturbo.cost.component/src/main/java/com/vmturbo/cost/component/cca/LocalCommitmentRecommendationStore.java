package com.vmturbo.cost.component.cca;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.pricing.RIPricingData;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence.CloudCommitmentRecommendationStore;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.ReservedInstanceRecommendationInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore.DemandType;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore.RIBuyInstanceDemand;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

/**
 * A local implementation of {@link CloudCommitmentRecommendationStore}, in which RIs are stored
 * within the {@link BuyReservedInstanceStore} and {@link ActionContextRIBuyStore}.
 */
public class LocalCommitmentRecommendationStore implements CloudCommitmentRecommendationStore {

    private final Logger logger = LogManager.getLogger();

    private final BuyReservedInstanceStore buyReservedInstanceStore;

    private final ActionContextRIBuyStore actionContextRIBuyStore;

    /**
     * Constructs a new {@link LocalCommitmentRecommendationStore} instance.
     * @param buyReservedInstanceStore The Buy RI store.
     * @param actionContextRIBuyStore The Buy RI instance demand store.
     */
    public LocalCommitmentRecommendationStore(@Nonnull BuyReservedInstanceStore buyReservedInstanceStore,
                                              @Nonnull ActionContextRIBuyStore actionContextRIBuyStore) {

        this.buyReservedInstanceStore = Objects.requireNonNull(buyReservedInstanceStore);
        this.actionContextRIBuyStore = Objects.requireNonNull(actionContextRIBuyStore);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void persistRecommendations(@Nonnull final CloudCommitmentAnalysisInfo analysisInfo,
                                       @Nonnull final Collection<CloudCommitmentRecommendation> recommendations,
                                       @Nonnull final MinimalCloudTopology<MinimalEntity> cloudTopology) {

        final SetMultimap<CloudCommitmentType, CloudCommitmentRecommendation> recommendationsByType =
                recommendations.stream()
                        .filter(CloudCommitmentRecommendation::isActionable)
                        .collect(ImmutableSetMultimap.toImmutableSetMultimap(
                                (recommendation) -> recommendation.recommendationInfo().commitmentType(),
                                Function.identity()));

        recommendationsByType.asMap().forEach((commitmentType, recommendationSet) -> {

            if (commitmentType == CloudCommitmentType.RESERVED_INSTANCE) {

                logger.info("Persisting {} RI buy recommendations", recommendationSet.size());

                persistReservedInstanceRecommendations(analysisInfo, recommendationSet);
                persistReservedInstanceDemand(analysisInfo, recommendationSet, cloudTopology);
            } else {
                throw new UnsupportedOperationException(
                        String.format("%s is not a supported cloud commitment type", commitmentType));
            }
        });
    }

    private void persistReservedInstanceRecommendations(@Nonnull final CloudCommitmentAnalysisInfo analysisInfo,
                                                        @Nonnull Collection<CloudCommitmentRecommendation> riRecommendations) {

        final Set<ReservedInstanceData> riDataSet = riRecommendations.stream()
                .map(this::convertRecommendationToRIData)
                .collect(ImmutableSet.toImmutableSet());

        buyReservedInstanceStore.updateBuyReservedInstances(
                riDataSet, analysisInfo.getAnalysisTopology().getTopologyContextId());

    }

    private ReservedInstanceData convertRecommendationToRIData(@Nonnull CloudCommitmentRecommendation recommendation) {

        final ReservedInstanceRecommendationInfo recommendationInfo =
                (ReservedInstanceRecommendationInfo)recommendation.recommendationInfo();
        final SavingsCalculationRecommendation savingsRecommendation = recommendation.savingsCalculationResult().recommendation();
        final ReservedInstanceSpecData riSpecData = recommendationInfo.commitmentSpecData();
        final RIPricingData riPricingData = recommendationInfo.cloudCommitmentPricingData();

        final ReservedInstanceBoughtCost riBoughtCost = ReservedInstanceBoughtCost.newBuilder()
                .setFixedCost(CurrencyAmount.newBuilder()
                        .setAmount(riPricingData.hourlyUpFrontRate() * riSpecData.termInHours())
                        .build())
                .setRecurringCostPerHour(CurrencyAmount.newBuilder()
                        .setAmount(riPricingData.hourlyRecurringRate())
                        .build())
                .build();
        final ReservedInstanceDerivedCost riDerivedCost = ReservedInstanceDerivedCost.newBuilder()
                .setAmortizedCostPerHour(
                        CurrencyAmount.newBuilder()
                                .setAmount(riPricingData.amortizedHourlyRate())
                                .build())
                .build();

        final long recommendationCouponCapacity =
                riSpecData.couponsPerInstance() * savingsRecommendation.recommendationQuantity();
        final ReservedInstanceBoughtCoupons riBoughtCoupons = ReservedInstanceBoughtCoupons.newBuilder()
                .setNumberOfCoupons(recommendationCouponCapacity)
                // utilization is represented as a number between 0.0 and 100.0
                .setNumberOfCouponsUsed(recommendationCouponCapacity * savingsRecommendation.recommendationUtilization() / 100)
                .build();

        final ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
                // TO make matching the action context demand data the RI bought instance,
                // the action and BuyRI instance have the same ID.
                .setId(recommendation.recommendationId())
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setBusinessAccountId(recommendationInfo.purchasingAccountOid())
                        .setNumBought((int)savingsRecommendation.recommendationQuantity())
                        .setReservedInstanceSpec(riSpecData.specId())
                        .setReservedInstanceBoughtCost(riBoughtCost)
                        .setReservedInstanceDerivedCost(riDerivedCost)
                        .setReservedInstanceBoughtCoupons(riBoughtCoupons))
                .build();

        return ReservedInstanceData.builder()
                .commitment(riBought)
                .spec(riSpecData.spec())
                .build();
    }

    private void persistReservedInstanceDemand(@Nonnull final CloudCommitmentAnalysisInfo analysisInfo,
                                               @Nonnull Collection<CloudCommitmentRecommendation> riRecommendations,
                                               @Nonnull final MinimalCloudTopology<MinimalEntity> cloudTopology) {

        final List<RIBuyInstanceDemand> instanceDemandList = riRecommendations.stream()
                .map(recommendation -> convertRIToInstanceDemand(analysisInfo, recommendation, cloudTopology))
                .flatMap(List::stream)
                .collect(ImmutableList.toImmutableList());

        actionContextRIBuyStore.insertRIBuyInstanceDemand(instanceDemandList);
    }

    private List<RIBuyInstanceDemand> convertRIToInstanceDemand(@Nonnull CloudCommitmentAnalysisInfo analysisInfo,
                                                                @Nonnull CloudCommitmentRecommendation recommendation,
                                                                @Nonnull final MinimalCloudTopology<MinimalEntity> cloudTopology) {

        final SavingsCalculationContext savingsContext = recommendation.savingsCalculationResult().recommendationContext();

        final Instant lastDatapointTime =
                savingsContext.demandSegments().last().timeInterval().startTime();
        final Map<Long, RIBuyInstanceDemand.Builder> instanceDemandByOid =
                recommendation.coveredDemandInfo().cloudTierSet()
                        .stream()
                        .map(ScopedCloudTierInfo::cloudTierDemand)
                        .map(CloudTierDemand::cloudTierOid)
                        .distinct()
                        .filter(cloudTopology::entityExists)
                        .collect(ImmutableMap.toImmutableMap(
                                Function.identity(),
                                (cloudTierOid) -> RIBuyInstanceDemand.builder()
                                        .topologyContextId(analysisInfo.getAnalysisTopology().getTopologyContextId())
                                        .actionId(recommendation.recommendationId())
                                        .demandType(DemandType.OBSERVED_DEMAND)
                                        .lastDatapointTime(
                                                LocalDateTime.ofInstant(lastDatapointTime, ZoneId.systemDefault()))
                                        .instanceType(cloudTopology.getEntity(cloudTierOid)
                                                .map(MinimalEntity::getDisplayName)
                                                .orElse(cloudTierOid.toString()))));

        savingsContext.demandSegments().forEach(demandSegment -> {

            // Collect all demand by tier OID. For a recommendation, there may be multiple demand instances
            // within the savings calculation context with the same tier OID (with differing platforms
            // or tenancy).
            final Map<Long, Double> uncoveredDemandByTierOid = demandSegment.tierDemandMap().entrySet()
                    .stream()
                    .collect(Collectors.groupingBy(
                            (e) -> e.getKey().cloudTierDemand().cloudTierOid(),
                            Collectors.mapping(
                                    (e) -> e.getValue().uncoveredDemand(),
                                    Collectors.reducing(0.0, Double::sum))));

            // check all instance demand builder. If there is nothing associated with the tier
            // in this demand segment, add a 0.
            instanceDemandByOid.forEach((tierOid, instanceDemandBuilder) ->
                    instanceDemandBuilder.addDatapoint(
                            // stats store float values - convert here and potentially lose some precision
                            uncoveredDemandByTierOid.getOrDefault(tierOid, 0.0).floatValue()));


        });

        return instanceDemandByOid.values()
                .stream()
                .map(RIBuyInstanceDemand.Builder::build)
                .collect(ImmutableList.toImmutableList());
    }

}
