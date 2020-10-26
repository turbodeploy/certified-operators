package com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence;

import static com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendationTest.RESERVED_INSTANCE_RECOMMENDATION;
import static com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendationTest.RESERVED_INSTANCE_SPEC_DATA;
import static com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendationTest.RESERVED_INSTANCE_SPEC_INFO;
import static com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendationTest.RI_SAVINGS_CALCULATION_RECOMMENDATION;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nonnull;

import org.junit.Before;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.BuyRIExplanation;

public class RecommendationActionTranslatorTest {

    private RecommendationActionTranslator actionTranslator;

    @Before
    public void setup() {
        actionTranslator = new RecommendationActionTranslator();
    }

    @Nonnull
    public void testReservedInstanceAction() {

        final Action action = actionTranslator.translateRecommendation(
                RESERVED_INSTANCE_RECOMMENDATION);

        assertThat(action.getInfo(), equalTo(RESERVED_INSTANCE_RECOMMENDATION.recommendationId()));
        // recommendation is 10 over 2 hours
        assertThat(action.getSavingsPerHour().getAmount(), closeTo(10.0, .001));
        assertTrue(action.getInfo().hasBuyRi());

        final BuyRI buyRI = action.getInfo().getBuyRi();
        assertThat(buyRI.getBuyRiId(), equalTo(RESERVED_INSTANCE_RECOMMENDATION.recommendationId()));
        assertTrue(buyRI.hasComputeTier());
        assertThat(buyRI.getComputeTier().getId(), equalTo(RESERVED_INSTANCE_SPEC_DATA.tierOid()));
        assertThat(buyRI.getCount(), equalTo(RI_SAVINGS_CALCULATION_RECOMMENDATION.recommendationQuantity()));
        assertTrue(buyRI.hasRegion());
        assertThat(buyRI.getRegion().getId(), equalTo(RESERVED_INSTANCE_SPEC_INFO.getRegionId()));
        assertTrue(buyRI.hasMasterAccount());
        assertThat(buyRI.getMasterAccount().getId(), equalTo(RESERVED_INSTANCE_RECOMMENDATION.recommendationInfo().purchasingAccountOid()));

        // test the explanation
        assertTrue(action.getExplanation().hasBuyRI());
        final BuyRIExplanation explanation = action.getExplanation().getBuyRI();
        // RI covers 4 coupons over 2 hours = 2 avg
        assertThat(explanation.getCoveredAverageDemand(), closeTo(2.0, .001));
        assertThat(explanation.getTotalAverageDemand(), closeTo(2.5, .001));
        // 42.5 in total on-demand cost / 2 hours * 730 hours * 12 months
        assertThat(explanation.getEstimatedOnDemandCost(), closeTo(186150, .001));
    }
}
