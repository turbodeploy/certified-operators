package com.vmturbo.action.orchestrator.store;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Unit tests for MarketActionConverter.
 */
public class MarketActionConverterTest {
    private static ActionInfo.Builder ACTION_INFO = ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                                 .setTarget(ActionEntity.newBuilder()
                                            .setId(343).setType(2)));

    /**
     * Test that an action that has no fields to migrate, remains intact.
     */
    @Test
    public void testNoMigration() {
        MarketActionConverter cvt = new MarketActionConverter();
        Action a = Action.newBuilder().setId(1l)
                        .setInfo(ACTION_INFO)
                        .setDeprecatedImportance(12.6f)
                        .setExplanation(Explanation.newBuilder()
                                        .setActivate(ActivateExplanation.newBuilder()
                                                     .setMostExpensiveCommodity(2))
                                        .build())
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .build();
        Assert.assertEquals(a, cvt.from(cvt.to(a)));
    }

    /**
     * Test that an action with both deprecated and new values, retains new field value.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testNoMigrationNewFieldsDefined() {
        MarketActionConverter cvt = new MarketActionConverter();
        int deprecatedType = 1;
        int newType = 2;
        Action a = Action.newBuilder().setId(1l)
                        .setExplanation(Explanation.newBuilder()
                                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                                        .addDeprecatedReconfigureCommodity(CommodityType
                                                                        .newBuilder()
                                                                        .setType(deprecatedType))
                                                        .addReconfigureCommodity(ReasonCommodity
                                                                        .newBuilder()
                                                                        .setCommodityType(CommodityType
                                                                                        .newBuilder()
                                                                                        .setType(newType)))))
                        .setInfo(ACTION_INFO)
                        .setDeprecatedImportance(12.6f)
                        .build();
        Action converted = cvt.from(cvt.to(a));

        Assert.assertNotNull(converted);
        Assert.assertEquals(a.getId(), converted.getId());
        Assert.assertTrue(converted.hasExplanation());
        Explanation exp = converted.getExplanation();
        Assert.assertTrue(exp.hasReconfigure());
        ReconfigureExplanation rexp = exp.getReconfigure();
        Assert.assertEquals(1, rexp.getReconfigureCommodityCount());
        CommodityType ct = rexp.getReconfigureCommodity(0).getCommodityType();
        Assert.assertNotNull(ct);
        Assert.assertEquals(newType, ct.getType());
    }

    /**
     * Test that a reconfigure action has commodity type migrated from deprecated value
     * when the new value is absent.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testReconfigureMigration() {
        MarketActionConverter cvt = new MarketActionConverter();
        Set<Integer> deprecated = ImmutableSet.of(5, 7);
        ReconfigureExplanation.Builder builder = ReconfigureExplanation.newBuilder();
        for (Integer ct : deprecated) {
            builder.addDeprecatedReconfigureCommodity(CommodityType
                                                      .newBuilder()
                                                      .setType(ct));
        }
        Action a = Action.newBuilder().setId(1l)
                        .setExplanation(Explanation.newBuilder()
                                        .setReconfigure(builder))
                        .setInfo(ACTION_INFO)
                        .setDeprecatedImportance(12.6f)
                        .build();
        Action converted = cvt.from(cvt.to(a));

        Assert.assertNotNull(converted);
        Assert.assertEquals(a.getId(), converted.getId());
        Assert.assertTrue(converted.hasExplanation());
        Explanation exp = converted.getExplanation();
        Assert.assertTrue(exp.hasReconfigure());
        ReconfigureExplanation rexp = exp.getReconfigure();
        Assert.assertEquals(deprecated.size(), rexp.getReconfigureCommodityCount());
        Set<Integer> convertedTypes = rexp.getReconfigureCommodityList().stream()
                        .map(ReasonCommodity::getCommodityType).map(CommodityType::getType)
                        .collect(Collectors.toSet());
        Assert.assertEquals(convertedTypes, deprecated);
    }

    /**
     * Test migration for a move action with several explanation parts
     * for congestion and efficiency at once.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMoveMigrationMultiPart() {
        MarketActionConverter cvt = new MarketActionConverter();
        int deprecatedCongestion = 12;
        int deprecatedEfficiency = 15;
        int newCompliance = 23;
        // deprecated
        Congestion.Builder congestion = Congestion.newBuilder()
                        .addDeprecatedCongestedCommodities(CommodityType.newBuilder()
                                        .setType(deprecatedCongestion));
        Efficiency.Builder efficiency = Efficiency.newBuilder()
                        .addDeprecatedUnderUtilizedCommodities(CommodityType.newBuilder()
                                        .setType(deprecatedEfficiency));
        // not deprecated
        Compliance.Builder compliance = Compliance.newBuilder()
                        .addMissingCommodities(ReasonCommodity.newBuilder()
                                        .setCommodityType(CommodityType.newBuilder()
                                                        .setType(newCompliance)));
        Action a = Action.newBuilder().setId(1l)
                        .setExplanation(Explanation.newBuilder()
                                        .setMove(MoveExplanation.newBuilder()
                                                 .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                                                               .setCongestion(congestion))
                                                 .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                                                               .setEfficiency(efficiency))
                                                 .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                                                               .setCompliance(compliance))
                                                 ))
                        .setInfo(ACTION_INFO)
                        .setDeprecatedImportance(12.6f)
                        .build();
        Action converted = cvt.from(cvt.to(a));

        Assert.assertNotNull(converted);
        Assert.assertEquals(a.getId(), converted.getId());
        Assert.assertTrue(converted.hasExplanation());
        Explanation exp = converted.getExplanation();
        Assert.assertTrue(exp.hasMove());
        MoveExplanation rexp = exp.getMove();
        Assert.assertEquals(3, rexp.getChangeProviderExplanationCount());

        ChangeProviderExplanation cpe1 = rexp.getChangeProviderExplanation(0);
        Assert.assertTrue(cpe1.hasCongestion());
        Congestion convertedCongestion = cpe1.getCongestion();
        Assert.assertEquals(1, convertedCongestion.getCongestedCommoditiesCount());
        Assert.assertEquals(deprecatedCongestion, convertedCongestion.getCongestedCommodities(0)
                        .getCommodityType().getType());

        ChangeProviderExplanation cpe2 = rexp.getChangeProviderExplanation(1);
        Assert.assertTrue(cpe2.hasEfficiency());
        Efficiency convertedEfficiency = cpe2.getEfficiency();
        Assert.assertEquals(1, convertedEfficiency.getUnderUtilizedCommoditiesCount());
        Assert.assertEquals(deprecatedEfficiency, convertedEfficiency.getUnderUtilizedCommodities(0)
                        .getCommodityType().getType());

        ChangeProviderExplanation cpe3 = rexp.getChangeProviderExplanation(2);
        Assert.assertTrue(cpe3.hasCompliance());
        Compliance convertedCompliance = cpe3.getCompliance();
        Assert.assertEquals(1, convertedCompliance.getMissingCommoditiesCount());
        Assert.assertEquals(newCompliance, convertedCompliance.getMissingCommodities(0)
                        .getCommodityType().getType());
    }

}
