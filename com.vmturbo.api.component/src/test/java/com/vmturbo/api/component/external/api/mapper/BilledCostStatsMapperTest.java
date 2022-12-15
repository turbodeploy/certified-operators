package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.mapper.cost.BilledCostStatsMapper;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStat;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Unit tests for {@link BilledCostStatsMapper}.
 */
public class BilledCostStatsMapperTest {

    @Mock
    private UuidMapper uuidMapper;

    private BilledCostStatsMapper statsMapper;

    /**
     * Set up.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        statsMapper = new BilledCostStatsMapper(uuidMapper);
    }

    /**
     * Tests conversion of tag values within a {@link BilledCostStat} to filter attributes within the {@link StatSnapshotApiDTO}.
     * @throws IOException Exception in loading the {@link BilledCostStat} data.
     */
    @Test
    public void testTagConversion() throws IOException {

        final List<BilledCostStat> billedCostStats = loadStatsList("/mapper/billedcost/cost_stat_tag.json");

        final List<StatSnapshotApiDTO> statSnapshotList = statsMapper.convertBilledCostStats(billedCostStats);

        // Assertions
        assertThat(statSnapshotList, hasSize(1));

        final StatSnapshotApiDTO actualSnapshotDto = statSnapshotList.get(0);
        assertThat(actualSnapshotDto.getDate(), equalTo("1970-01-01T01:00:00"));
        assertThat(actualSnapshotDto.getEpoch(), equalTo(Epoch.HISTORICAL));

        // stats assertions
        assertThat(actualSnapshotDto.getStatistics(), hasSize(1));
        final StatApiDTO actualStatDto = actualSnapshotDto.getStatistics().get(0);

        final StatValueApiDTO actualValues = actualStatDto.getValues();
        assertThat(actualValues.getMax(), equalTo(6.0f));
        assertThat(actualValues.getMin(), equalTo(2.0f));
        assertThat(actualValues.getAvg(), equalTo(4.0f));
        assertThat(actualValues.getTotal(), equalTo(8.0f));

        // Filter assertions
        assertThat(actualStatDto.getFilters(), hasSize(2));

        final StatFilterApiDTO firstFilter = actualStatDto.getFilters().get(0);
        assertThat(firstFilter.getType(), equalTo("tagKey"));
        assertThat(firstFilter.getValue(), equalTo("tagKeyTest"));

        final StatFilterApiDTO secondFilter = actualStatDto.getFilters().get(1);
        assertThat(secondFilter.getType(), equalTo("tagValue"));
        assertThat(secondFilter.getValue(), equalTo("tagValueTest"));
    }

    /**
     * Test that the results returned by
     * {@link BilledCostStatsMapper#convertBilledCostStats(Collection)} are in date order from
     * earliest to latest.
     */
    @Test
    public void testBilledCostStatsInDateOrder() throws IOException {
        final String date1 = "1670716800000"; // 2022-12-11T00:00:00
        final String date2 = "1670803200000"; // 2022-12-12T00:00:00
        final String date3 = "1670889600000"; // 2022-12-13T00:00:00
        final BilledCostStat stat1 = BilledCostStat.newBuilder().setSampleTsUtc(
                DateTimeUtil.parseTime(date1)).build();
        final BilledCostStat stat2 = BilledCostStat.newBuilder().setSampleTsUtc(
                DateTimeUtil.parseTime(date2)).build();
        final BilledCostStat stat3 = BilledCostStat.newBuilder().setSampleTsUtc(
                DateTimeUtil.parseTime(date3)).build();

        final List<StatSnapshotApiDTO> statSnapshotList = statsMapper.convertBilledCostStats(
                Arrays.asList(stat2, stat3, stat1));
        assertThat(statSnapshotList, hasSize(3));

        final TimeZone utc = TimeZone.getTimeZone("UTC");
        assertThat(statSnapshotList.get(0).getDate(),
                equalTo(DateTimeUtil.toString(Long.parseLong(date1), utc)));
        assertThat(statSnapshotList.get(1).getDate(),
                equalTo(DateTimeUtil.toString(Long.parseLong(date2), utc)));
        assertThat(statSnapshotList.get(2).getDate(),
                equalTo(DateTimeUtil.toString(Long.parseLong(date3), utc)));
    }

    /**
     * Tests resolving OID references within {@link BilledCostStat}.
     * @throws IOException Exception in loading the {@link BilledCostStat} data.
     */
    @Test
    public void testRelatedEntityStat() throws IOException {
        final List<BilledCostStat> billedCostStats = loadStatsList("/mapper/billedcost/cost_stat_entity.json");

        // set up the uuid mapper
        final CachedEntityInfo entityInfo = mock(CachedEntityInfo.class);
        when(entityInfo.getDisplayName()).thenReturn("entityDisplayNameTest");
        when(entityInfo.getEntityType()).thenReturn(ApiEntityType.VIRTUAL_MACHINE);

        final CachedEntityInfo accountInfo = mock(CachedEntityInfo.class);
        when(accountInfo.getDisplayName()).thenReturn("accountDisplayNameTest");

        final CachedEntityInfo regionInfo = mock(CachedEntityInfo.class);
        when(regionInfo.getDisplayName()).thenReturn("regionDisplayNameTest");

        final CachedEntityInfo serviceProviderInfo = mock(CachedEntityInfo.class);
        when(serviceProviderInfo.getDisplayName()).thenReturn("serviceProviderDisplayNameTest");

        when(uuidMapper.bulkResolveEntityInfo(any())).thenReturn(ImmutableMap.of(
                1L, entityInfo,
                2L, accountInfo,
                3L, regionInfo,
                4L, serviceProviderInfo));

        // Invoke the mapper
        final List<StatSnapshotApiDTO> statSnapshotList = statsMapper.convertBilledCostStats(billedCostStats);

        // Assertions
        assertThat(statSnapshotList, hasSize(1));
        final StatSnapshotApiDTO actualSnapshotDto = statSnapshotList.get(0);

        // stats assertions
        assertThat(actualSnapshotDto.getStatistics(), hasSize(1));
        final StatApiDTO actualStatDto = actualSnapshotDto.getStatistics().get(0);

        assertThat(actualStatDto.getRelatedEntityType(), equalTo(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(actualStatDto.getRelatedEntity(), notNullValue());
        assertThat(actualStatDto.getRelatedEntity().getDisplayName(), equalTo("entityDisplayNameTest"));

        // Entity, entity type, account, region, service provider = 5
        assertThat(actualStatDto.getFilters(), hasSize(5));

        // Expected filters
        final StatFilterApiDTO entityFilter = createStatFilter(StringConstants.ENTITY, "entityDisplayNameTest");
        final StatFilterApiDTO entityTypeFilter = createStatFilter("EntityType", "VIRTUAL_MACHINE");
        final StatFilterApiDTO accountFilter = createStatFilter(ApiEntityType.BUSINESS_ACCOUNT.apiStr(), "accountDisplayNameTest");
        final StatFilterApiDTO regionFilter = createStatFilter(ApiEntityType.REGION.apiStr(), "regionDisplayNameTest");
        final StatFilterApiDTO serviceProviderFilter = createStatFilter(ApiEntityType.SERVICE_PROVIDER.apiStr(), "serviceProviderDisplayNameTest");

        assertThat(actualStatDto.getFilters(),
                containsInAnyOrder(entityFilter, entityTypeFilter, accountFilter, regionFilter, serviceProviderFilter));
    }

    private StatFilterApiDTO createStatFilter(@Nonnull String type,
                                              @Nonnull String value) {
        final StatFilterApiDTO filter = new StatFilterApiDTO();
        filter.setType(type);
        filter.setValue(value);

        return filter;
    }

    private List<BilledCostStat> loadStatsList(@Nonnull String statProtoFilepath) throws IOException {

        final ObjectMapper objectMapper = new ObjectMapper();
        final ImmutableList.Builder<BilledCostStat> costStatsList = ImmutableList.builder();
        try (InputStream is = BilledCostStatsMapper.class.getResourceAsStream(statProtoFilepath)) {

            final List<JsonNode> statsNodeList = objectMapper.readValue(is, new TypeReference<List<JsonNode>>(){});

            for (JsonNode statNode : statsNodeList) {
                final BilledCostStat.Builder costStat = BilledCostStat.newBuilder();
                JsonFormat.parser().merge(statNode.toString(), costStat);
                costStatsList.add(costStat.build());
            }
        }

        return costStatsList.build();
    }
}
