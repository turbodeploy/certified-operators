package com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.ReservedInstanceRecommendationInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationRecommendation;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.BuyRIExplanation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * A translator of {@link CloudCommitmentRecommendation} instances to {@link Action} instances (for
 * action plan broadcasts).
 */
public class RecommendationActionTranslator {

    private final Logger logger = LogManager.getLogger();

    /**
     * Translates {@code recommendation} to an {@link Action} instance.
     * @param recommendation The cloud commitment recommendation to translate.
     * @return The action translation.
     */
    @Nonnull
    public Action translateRecommendation(@Nonnull CloudCommitmentRecommendation recommendation) {

        logger.debug("Translating recommendation to action (Recommendation Info={})", recommendation.recommendationInfo());

        final ActionInfo actionInfo;
        final CloudCommitmentType cloudCommitmentType = recommendation.recommendationInfo().commitmentType();
        if (cloudCommitmentType == CloudCommitmentType.RESERVED_INSTANCE) {
            actionInfo = createBuyRIActionInfo(recommendation);
        } else {
            throw new UnsupportedOperationException(String.format(
                    "%s is not a supported cloud commitment type", cloudCommitmentType));
        }

        final Explanation explanation = createActionExplanation(recommendation);
        final double hourlySavings = calculateHourlySavings(recommendation);

        final ActionDTO.Action action =
                ActionDTO.Action.newBuilder()
                        // Using the recommendation ID makes it easier to map the action to the context
                        // data. Having the same ID referenced in the action and Buy RI instance
                        // should not cause a conflict.
                        .setId(recommendation.recommendationId())
                        .setInfo(actionInfo)
                        .setExplanation(explanation)
                        .setSupportingLevel(SupportLevel.SHOW_ONLY)
                        .setSavingsPerHour(CurrencyAmount.newBuilder()
                                .setAmount(hourlySavings).build())
                        .setExecutable(false)
                        // required, even though it's deprecated
                        .setDeprecatedImportance(0)
                        .build();

        return action;
    }

    @Nonnull
    private ActionInfo createBuyRIActionInfo(@Nonnull CloudCommitmentRecommendation recommendation) {

        final ReservedInstanceRecommendationInfo recommendationInfo =
                (ReservedInstanceRecommendationInfo)recommendation.recommendationInfo();
        final SavingsCalculationRecommendation savingsRecommendation =
                recommendation.savingsCalculationResult().recommendation();

        final BuyRI buyRI = BuyRI.newBuilder()
                .setBuyRiId(recommendation.recommendationId())
                .setComputeTier(ActionEntity.newBuilder()
                        .setId(recommendationInfo.tierOid())
                        .setType(recommendationInfo.tierType())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD))
                .setCount((int)savingsRecommendation.recommendationQuantity())
                .setRegion(ActionEntity.newBuilder()
                        .setId(recommendationInfo.regionOid())
                        .setType(EntityType.REGION_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build())
                .setMasterAccount(ActionEntity.newBuilder()
                        .setId(recommendationInfo.purchasingAccountOid())
                        .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build())
                .build();

        return ActionInfo.newBuilder()
                .setBuyRi(buyRI)
                .build();
    }

    private Explanation createActionExplanation(@Nonnull CloudCommitmentRecommendation recommendation) {

        final CloudCommitmentSpecData commitmentSpecData = recommendation.recommendationInfo().commitmentSpecData();
        final SavingsCalculationRecommendation savingsRecommendation =
                recommendation.savingsCalculationResult().recommendation();

        // Assumes the analysis window will always be a multiple of an hour.
        final long hourNormalizationFactor = savingsRecommendation.appraisalDuration().toHours();
        final double averageCoveredDemand = savingsRecommendation.coveredDemand() / hourNormalizationFactor;
        // Buy RI actions are only relative to the uncovered demand after inventory is applied. Therefore,
        // this stat only includes the uncovered demand.
        final double totalAverageDemand = savingsRecommendation.initialUncoveredDemand() / hourNormalizationFactor;

        // unlike the other two, this one isn't hourly for some reason
        final double estimatedOnDemandCost = savingsRecommendation.aggregateOnDemandCost() / hourNormalizationFactor
                * commitmentSpecData.termInHours();

        return Explanation.newBuilder()
                .setBuyRI(BuyRIExplanation.newBuilder()
                        .setCoveredAverageDemand(averageCoveredDemand)
                        .setTotalAverageDemand(totalAverageDemand)
                        .setEstimatedOnDemandCost(estimatedOnDemandCost)
                        .build())
                .build();
    }

    private double calculateHourlySavings(@Nonnull CloudCommitmentRecommendation recommendation) {

        final SavingsCalculationRecommendation savingsRecommendation =
                recommendation.savingsCalculationResult().recommendation();

        // Assumes the analysis window will always be a multiple of an hour.
        final long hourNormalizationFactor = savingsRecommendation.appraisalDuration().toHours();
        // The recommendation savings will be for the entire appraisal window (e.g. 30 days).
        return savingsRecommendation.savings() / hourNormalizationFactor;
    }
}
