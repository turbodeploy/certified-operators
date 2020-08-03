package com.vmturbo.history.stats.live;

import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_MONTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;

import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.stats.live.StatsQueryFactory.DefaultStatsQueryFactory;

public class StatsQueryFactoryTest {

    @Test
    public void testGetStatsCommoditySelect() throws Exception {
        final HistorydbIO historydbIO = mock(HistorydbIO.class);
        final StatsQueryFactory statsQueryFactory = new DefaultStatsQueryFactory(historydbIO);
        final List<CommodityRequest> commodityRequests = Lists.newArrayList(
                CommodityRequest.newBuilder()
                        .setCommodityName("X")
                        .addPropertyValueFilter(
                                Stats.StatsFilter.PropertyValueFilter.newBuilder()
                                        .setProperty("relation")
                                        .setValue("bought")
                                        .build())
                        .build(),
                CommodityRequest.newBuilder()
                        .setCommodityName("Y")
                        .build()
        );
        final String X_TEST =
            DSL.trueCondition()
                .and(PM_STATS_BY_MONTH.PROPERTY_TYPE.eq("X"))
                .and(PM_STATS_BY_MONTH.RELATION.eq(RelationType.COMMODITIESBOUGHT)).toString()
                .replaceAll("\n", "")
                .replaceAll(" ", "");
        final String Y_TEST = PM_STATS_BY_MONTH.PROPERTY_TYPE.eq("Y").toString()
            .replaceAll("\n", "")
            .replaceAll(" ", "");

        // act
        Optional<Condition> condition = statsQueryFactory.createCommodityRequestsCond(commodityRequests, PM_STATS_BY_MONTH);

        assertTrue(condition.isPresent());
        final String propertiesConditionsString = condition.get().toString()
            .replaceAll("\n", "")
            .replaceAll(" ", "");
        assertThat(propertiesConditionsString, containsString(X_TEST));
        assertThat(propertiesConditionsString, containsString(Y_TEST));
    }

    @Test
    /**
     * Test that the related entity type is also included in the where clause.  This is specifically
     * the case when the query is against the market stats tables, not entity type specific stats
     */
    public void testGetStatsCommoditySelectWMultipleRelatedEntityType() throws Exception {
        final HistorydbIO historydbIO = mock(HistorydbIO.class);
        final StatsQueryFactory statsQueryFactory = new DefaultStatsQueryFactory(historydbIO);
        final List<CommodityRequest> commodityRequests = Lists.newArrayList(
                CommodityRequest.newBuilder()
                        .setCommodityName("X")
                        .addPropertyValueFilter(
                                Stats.StatsFilter.PropertyValueFilter.newBuilder()
                                        .setProperty("relation")
                                        .setValue("bought")
                                        .build())
                        .setRelatedEntityType("VirtualMachine")
                        .build(),
                CommodityRequest.newBuilder()
                        .setCommodityName("Y")
                        .setRelatedEntityType("PhysicalMachine")
                        .build()
        );
        final String X_TEST =
                DSL.trueCondition()
                        .and(MARKET_STATS_LATEST.PROPERTY_TYPE.eq("X"))
                        .and(MARKET_STATS_LATEST.ENTITY_TYPE.in("VirtualMachine"))
                        .and(MARKET_STATS_LATEST.RELATION.eq(RelationType.COMMODITIESBOUGHT)).toString()
                        .replaceAll("\n", "")
                        .replaceAll(" ", "");
        final String Y_TEST = DSL.trueCondition()
                .and(MARKET_STATS_LATEST.PROPERTY_TYPE.eq("Y"))
                .and(MARKET_STATS_LATEST.ENTITY_TYPE.in("PhysicalMachine")).toString()
                .replaceAll("\n", "")
                .replaceAll(" ", "");

        // act
        Optional<Condition> condition = statsQueryFactory.createCommodityRequestsCond(commodityRequests, Tables.MARKET_STATS_LATEST);

        assertTrue(condition.isPresent());
        final String propertiesConditionsString = condition.get().toString()
                .replaceAll("\n", "")
                .replaceAll(" ", "");
        assertThat(propertiesConditionsString, containsString(X_TEST));
        assertThat(propertiesConditionsString, containsString(Y_TEST));
    }

    /**
     * Should return empty optional when env type hybrid.
     */
    @Test
    public void testEnvironmentTypeCondWithHybrid() {
        //GIVEN
        final EnvironmentType environmentType = EnvironmentType.HYBRID;
        final StatsQueryFactory statsQueryFactory = new DefaultStatsQueryFactory(mock(HistorydbIO.class));

        //WHEN
        final Optional<Condition> response = statsQueryFactory.environmentTypeCond(environmentType, MARKET_STATS_LATEST);

        //THEN
        assertFalse(response.isPresent());
    }

    /**
     * Should return non empty optional when env type on-prem.
     */
    @Test
    public void testEnvironmentTypeCondWithOnPrem() {
        //GIVEN
        final EnvironmentType environmentType = EnvironmentType.ON_PREM;
        final StatsQueryFactory statsQueryFactory = new DefaultStatsQueryFactory(mock(HistorydbIO.class));

        //WHEN
        final Optional<Condition> response = statsQueryFactory.environmentTypeCond(environmentType, MARKET_STATS_LATEST);

        //THEN
        assertTrue(response.isPresent());
    }

    /**
     * Should return empty optional when table is not market.
     */
    @Test
    public void testEnvironmentTypeCondWithOnPremAndNonMarketTable() {
        //GIVEN
        final EnvironmentType environmentType = EnvironmentType.HYBRID;
        final StatsQueryFactory statsQueryFactory = new DefaultStatsQueryFactory(mock(HistorydbIO.class));

        //WHEN
        final Optional<Condition> response = statsQueryFactory.environmentTypeCond(environmentType,
                Tables.CLUSTER_STATS_BY_DAY);

        //THEN
        assertFalse(response.isPresent());
    }

}
