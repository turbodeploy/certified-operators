package com.vmturbo.api.component.mapper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.util.Lists;
import org.assertj.core.util.Sets;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.service.StatsService;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.reports.db.RelationType;

/**
 * Unit tests for the static Mapper utility functions for the {@link StatsService}.
 */
public class StatsMapperTest {

    public static final long START_DATE = 1234L;
    public static final long END_DATE = 5678L;

    /**
     * Test Conversion of gRPC stats call result to the ApiDTO to return for the REST API caller.
     */
    @Test
    public void toStatSnapshotApiDTOTest() throws Exception {
        String[] postfixes = {"A", "B", "C"};
        String[] relations = {RelationType.COMMODITIES.getLiteral(),
                              RelationType.COMMODITIESBOUGHT.getLiteral(),
                              RelationType.COMMODITIES_FROM_ATTRIBUTES.getLiteral()};

        // Arrange
        Stats.StatSnapshot testSnapshot = Stats.StatSnapshot.newBuilder()
                .setSnapshotDate("date-value")
                .setStartDate(1234L)
                .setEndDate(5678L)
                .addAllStatRecords(buildStatRecords(postfixes, relations))
                .build();

        // Act
        StatSnapshotApiDTO mapped = StatsMapper.toStatSnapshotApiDTO(testSnapshot);
        // Assert
        assertThat(testSnapshot.getSnapshotDate(), is(mapped.getDate()));
        assertThat(testSnapshot.getStatRecordsCount(), is(mapped.getStatistics().size()));
        assertEquals(3, testSnapshot.getStatRecordsCount());
        verifyMappedStatRecord(testSnapshot.getStatRecords(0), mapped.getStatistics().get(0),
                               "sold");
        verifyMappedStatRecord(testSnapshot.getStatRecords(1), mapped.getStatistics().get(1),
                               "bought");
        verifyMappedStatRecord(testSnapshot.getStatRecords(2), mapped.getStatistics().get(2),
                               "attribute");
    }

    @Test (expected = IllegalArgumentException.class)
    public void toStatSnapshotApiDTOWrongStatRelationTest() throws Exception {
        String[] postfixes = {"A"};
        String[] relations = {"WrongStatRelation"};

        // Arrange
        Stats.StatSnapshot testSnapshot = Stats.StatSnapshot.newBuilder()
                .setSnapshotDate("date-value")
                .setStartDate(START_DATE)
                .setEndDate(END_DATE)
                .addAllStatRecords(buildStatRecords(postfixes, relations))
                .build();

        // Act
        StatsMapper.toStatSnapshotApiDTO(testSnapshot);
    }
    @Test
    public void toStatApiDTOStatKeyFilter() throws Exception {
        final String statKey = "foo";
        StatSnapshot snapshot = StatSnapshot.newBuilder()
                .addStatRecords(StatSnapshot.StatRecord.newBuilder()
                        .setStatKey(statKey))
                .build();

        final StatSnapshotApiDTO dto = StatsMapper.toStatSnapshotApiDTO(snapshot);
        assertThat(dto.getStatistics().size(), is(1));
        assertThat(dto.getStatistics().get(0).getFilters().size(), is(1));
        StatFilterApiDTO filter = dto.getStatistics().get(0).getFilters().get(0);
        assertThat(filter.getType(), is(StatsMapper.FILTER_NAME_KEY));
        assertThat(filter.getValue(), is(statKey));
    }
    @Test
    public void testMetricsDoNotIncludeCapacityOrReserved() throws Exception {
        // Price index is a metric and metrics should not include capacities or reserved
        // or else the UI will render them as commodities with donut charts and utilizations.
        final String statMetricName = StatsMapper.METRIC_NAMES.iterator().next();
        StatSnapshot snapshot = StatSnapshot.newBuilder()
                .addStatRecords(StatSnapshot.StatRecord.newBuilder()
                        .setName(statMetricName))
                .build();

        final StatApiDTO dto = StatsMapper.toStatSnapshotApiDTO(snapshot).getStatistics().get(0);
        assertNull(dto.getCapacity());
        assertNull(dto.getReserved());
    }

    @Test
    public void testToEntityStatsRequest() {
        // Arrange
        Set<Long> entityOids = Sets.newLinkedHashSet(1L, 2L);
        StatPeriodApiInputDTO apiRequestInput = new StatPeriodApiInputDTO();
        apiRequestInput.setStartDate(Long.toString(START_DATE));
        apiRequestInput.setEndDate(Long.toString(END_DATE));

        // first stat to fetch
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName("stat1");
        statApiInputDTO.setRelatedEntityType("relatedType1");
        StatFilterApiDTO statFilterApiDTO1 = new StatFilterApiDTO();
        statFilterApiDTO1.setType("filter-type-1");
        statFilterApiDTO1.setValue("filter-value-1");

        StatFilterApiDTO statFilterApiDTO2 = new StatFilterApiDTO();
        statApiInputDTO.setRelatedEntityType("relatedType1");

        statFilterApiDTO2.setType("filter-type-2");
        statFilterApiDTO2.setValue("filter-value-2");
        statApiInputDTO.setFilters(Lists.newArrayList(statFilterApiDTO1, statFilterApiDTO2));

        // second stat to fetch
        StatApiInputDTO statApiInputDTO2 = new StatApiInputDTO();
        statApiInputDTO2.setName("stat2");
        apiRequestInput.setStatistics(Lists.newArrayList(statApiInputDTO, statApiInputDTO2));

        // Act
        EntityStatsRequest requestProtobuf = StatsMapper.toEntityStatsRequest(entityOids,
                apiRequestInput, Optional.empty());

        // Assert
        assertThat(requestProtobuf.getEntitiesCount(), equalTo(entityOids.size()));
        assertTrue(requestProtobuf.hasFilter());
        final Stats.StatsFilter filter = requestProtobuf.getFilter();
        assertThat(filter.getStartDate(), equalTo(Long.valueOf(apiRequestInput.getStartDate())));
        assertThat(filter.getEndDate(), equalTo(Long.valueOf(apiRequestInput.getEndDate())));
        assertThat(filter.getCommodityRequestsCount(), equalTo(2));
        assertThat(filter.getCommodityRequestsList(), containsInAnyOrder(
                Stats.StatsFilter.CommodityRequest.newBuilder()
                        .setCommodityName("stat1")
                        .addPropertyValueFilter(Stats.StatsFilter.PropertyValueFilter.newBuilder()
                                .setProperty("filter-type-1")
                                .setValue("filter-value-1")
                                .build())
                        .addPropertyValueFilter(Stats.StatsFilter.PropertyValueFilter.newBuilder()
                                .setProperty("filter-type-2")
                                .setValue("filter-value-2")
                                .build())
                        .setRelatedEntityType("relatedType1")
                        .build(),
                Stats.StatsFilter.CommodityRequest.newBuilder()
                        .setCommodityName("stat2")
                        .build()
        ));
    }

    @Test
    public void testToEntityStatsRequestDefaults() {
        // Arrange
        Set<Long> entityOids = Sets.newLinkedHashSet(1L, 2L);
        StatPeriodApiInputDTO apiRequestInput = new StatPeriodApiInputDTO();

        // Act
        EntityStatsRequest requestProtobuf = StatsMapper.toEntityStatsRequest(entityOids,
                apiRequestInput, Optional.empty());

        // Assert
        assertThat(requestProtobuf.getEntitiesCount(), equalTo(entityOids.size()));
        assertTrue(requestProtobuf.hasFilter());
        final Stats.StatsFilter filter = requestProtobuf.getFilter();
        assertFalse(filter.hasStartDate());
        assertFalse(filter.hasEndDate());
        assertThat(filter.getCommodityRequestsCount(), equalTo(0));
        assertThat(filter.getCommodityRequestsCount(), equalTo(0));
        assertThat(filter.getCommodityAttributesCount(), equalTo(0));
    }

    private void verifyMappedStatRecord(StatRecord test,
                                        StatApiDTO mappedStat,
                                        String relationshipType) {
        assertThat(mappedStat.getName(), is(test.getName()));
        assertThat(mappedStat.getReserved().getTotal(), is(test.getReserved()));

        assertEquals(mappedStat.getCapacity().getMin(), test.getCapacity().getMin(), 0);
        assertEquals(mappedStat.getCapacity().getMax(), test.getCapacity().getMax(), 0);
        assertEquals(mappedStat.getCapacity().getAvg(), test.getCapacity().getAvg(), 0);
        assertEquals(mappedStat.getCapacity().getTotal(), test.getCapacity().getTotal(), 0);

        // Check the relationship type type.
        if (relationshipType.equals("bought") || relationshipType.equals("sold")) {
            assertNotNull(mappedStat.getFilters());
            assertEquals(1, mappedStat.getFilters().size());
            assertEquals(StatsMapper.RELATION_FILTER_TYPE, mappedStat.getFilters().get(0).getType());
            assertEquals(relationshipType, mappedStat.getFilters().get(0).getValue());
        } else {
            assertTrue(mappedStat.getFilters() == null || mappedStat.getFilters().isEmpty());
        }

        assertThat(mappedStat.getRelatedEntity().getDisplayName(), is(test.getProviderDisplayName()));
        assertThat(mappedStat.getRelatedEntity().getUuid(), is(test.getProviderUuid()));
        assertThat(mappedStat.getUnits(), is(test.getUnits()));
        assertThat(mappedStat.getValue(), is(test.getUsed().getAvg()));
        validateStatValue(mappedStat.getValues(), test.getUsed());
    }

    /**
     * Build a list of StatRecord objects initialized based on the given postfixes.
     *
     * String fields are initialized with the field name plus "-" plus the postfix.
     * Numeric fields are initialized with the
     *
     * @param postfixes an array of strings to use as postfixes for new instances of StatRecord
     * @param relations an array of strings (of the same size as postfixes) to use as relations
     *        for new instances of StatRecord
     * @return a list of new StatRecord objects with fields initialized based on the given postfix
     */
    private List<StatRecord> buildStatRecords(String[] postfixes, String[] relations) {
        List<StatRecord> records = new ArrayList<>();
        for (int i = 0; i < postfixes.length; i++) {
            records.add(buildStatRecord(i, postfixes[i], relations[i]));
        }
        return records;
    }

    /**
     * Validate the fields of a {@link StatValueApiDTO} mapped from the original {@link StatValue}.
     *
     * @param mapped the {@link StatValueApiDTO} output of the mapping process
     * @param original the {@link StatValue} being mapped from
     */
    private void validateStatValue(StatValueApiDTO mapped, StatValue original) {
        assertThat(mapped.getMin(), is(original.getMin()));
        assertThat(mapped.getMax(), is(original.getMax()));
        assertThat(mapped.getAvg(), is(original.getAvg()));
        assertThat(mapped.getTotal(), is(original.getTotal()));
    }

    /**
     * Create a test StatRecord populated with values based on the postfix for string fields and the index for
     * numeric fields.
     *
     * @param index an ascending index to be used to salt numeric fields.
     * @param postfix a string postfix to be added to string-based fields.
     * @return a newly initialized StatRecord initialized based on the input index and postfix.
     */
    private StatRecord buildStatRecord(int index, String postfix, String relation) {
        return StatRecord.newBuilder()
            .setName("name-" + postfix)
            .setProviderUuid("puid-" + postfix)
            .setProviderDisplayName("provider-" + postfix)
            .setCapacity(buildStatValue(1000 + index))
            .setReserved(2000 + index)
            .setCurrentValue(3000 + index)
            .setPeak(buildStatValue(index))
            .setUsed(buildStatValue(index + 100))
            .setValues(buildStatValue(index + 200))
            .setRelation(relation)
            .build();
    }

    private static StatValue buildStatValue(int seed) {
        return StatValue.newBuilder()
                .setTotal(4000+seed)
                .setMax(5000+seed)
                .setMin(6000+seed)
                .setAvg(7000+seed)
                .build();
    }
}