package com.vmturbo.history.stats.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_MONTH;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.jooq.Condition;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.schema.RelationType;
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
        final String X_TEST = PM_STATS_BY_MONTH.PROPERTY_TYPE.eq("X")
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
}
