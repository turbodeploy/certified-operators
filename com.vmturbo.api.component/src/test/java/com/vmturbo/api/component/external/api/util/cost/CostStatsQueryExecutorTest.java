package com.vmturbo.api.component.external.api.util.cost;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.cost.CostInputApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.CostGroupBy;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot.StatRecord.TagKeyValuePair;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.TagFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.StatValue;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test for CostStatsQueryExecutor.
 */
public class CostStatsQueryExecutorTest {
    private CostStatsQueryExecutor costStatsQueryExecutor;

    private static final String TAG_KEY = "Owner";
    private static final String TAG_VALUE1 = "owner1";
    private static final String TAG_VALUE2 = "owner2";
    private static final long ENTITY_OID = 111;
    private static final long START_DATE = 1636719486000L;
    private static final long END_DATE = 1636719586000L;

    /**
     * check exception thrown.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final CostServiceMole costServiceMole = spy(new CostServiceMole());

    /**
     * wrapper with built-in handling of names and channels.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(costServiceMole);

    /**
     * set up.
     */
    @Before
    public void setup() {
        final CostServiceBlockingStub costServiceRpc = CostServiceGrpc.newBlockingStub(
                testServer.getChannel());
        final PaginationMapper paginationMapper = mock(PaginationMapper.class);
        final StatsMapper statsMapper = new StatsMapper(paginationMapper);
        costStatsQueryExecutor = new CostStatsQueryExecutor(costServiceRpc, statsMapper);
    }

    /**
     * Test building GetCloudBilledStatsRequest given costInputApiDTO.
     */
    @Test
    public void testBuildGetCloudBilledStatsRequest() {
        final GetCloudBilledStatsRequest.Builder builder = costStatsQueryExecutor
                .buildGetCloudBilledStatsRequest(createCostInputApiDTO(Lists.newArrayList(createTagApiDTO())));
        assertEquals(1, builder.getGroupByCount());
        assertEquals(GroupByType.TAG, builder.getGroupBy(0));
        assertEquals(1, builder.getTagFilterCount());
        final TagFilter tagFilter = builder.getTagFilter(0);
        assertEquals(TAG_KEY, tagFilter.getTagKey());
        assertThat(tagFilter.getTagValueList(), containsInAnyOrder(TAG_VALUE1, TAG_VALUE2));
    }

    /**
     * Test exception throw when inputDTO contains more than one tag filters.
     */
    @Test
    public void testTwoTagFiltersNotSupported() {
        final TagApiDTO tagApiDTO = createTagApiDTO();
        expectedException.expect(UnsupportedOperationException.class);
        costStatsQueryExecutor
                .buildGetCloudBilledStatsRequest(createCostInputApiDTO(Lists.newArrayList(tagApiDTO, tagApiDTO)));
    }

    /**
     * Test convert GetCloudBilledStatsResponse to a list of StatSnapshotApiDTOs as result.
     */
    @Test
    public void testBuildRequestGetCloudBilledStatsResponse() {
        when(costServiceMole.getCloudBilledStats(any())).thenReturn(buildGetCloudBilledStatsResponse());
        final List<StatSnapshotApiDTO> result = costStatsQueryExecutor.queryCostServiceAndConvert(GetCloudBilledStatsRequest.getDefaultInstance());
        // two output DTOs with different dates
        assertEquals(2, result.size());
        result.forEach(dto -> {
            assertEquals(1, dto.getStatistics().size());
            final StatApiDTO statApiDTO = dto.getStatistics().get(0);
            // for each stat, two filters, one for tag key, one for tag value
            assertEquals(2, statApiDTO.getFilters().size());
        });
    }

    /**
     * Test building request with entity filter.
     */
    @Test
    public void testBuildRequestWithEntityFilter() {
        final GetCloudBilledStatsRequest.Builder expectedResultBuilder = GetCloudBilledStatsRequest
                .newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(ENTITY_OID)
                        .build());
        testBuildRequestForEntityType(EntityType.VIRTUAL_MACHINE, expectedResultBuilder);
    }

    /**
     * Test building request with region filter.
     */
    @Test
    public void testBuildRequestWithRegionFilter() {
        final GetCloudBilledStatsRequest.Builder expectedResultBuilder = GetCloudBilledStatsRequest
                .newBuilder()
                .setRegionFilter(RegionFilter.newBuilder()
                        .addRegionId(ENTITY_OID)
                        .build());
        testBuildRequestForEntityType(EntityType.REGION, expectedResultBuilder);
    }

    /**
     * Test building request with account filter.
     */
    @Test
    public void testBuildRequestWithAccountFilter() {
        final GetCloudBilledStatsRequest.Builder expectedResultBuilder = GetCloudBilledStatsRequest
                .newBuilder()
                .setAccountFilter(AccountFilter.newBuilder()
                        .addAccountId(ENTITY_OID)
                        .build());
        testBuildRequestForEntityType(EntityType.BUSINESS_ACCOUNT, expectedResultBuilder);
    }

    private void testBuildRequestForEntityType(
            @Nonnull final EntityType entityType,
            @Nonnull final GetCloudBilledStatsRequest.Builder expectedResultBuilder) {
        // ARRANGE
        final CostInputApiDTO costInputApiDTO = new CostInputApiDTO();
        costInputApiDTO.setStartDate(DateTimeUtil.toString(START_DATE));
        costInputApiDTO.setEndDate(DateTimeUtil.toString(END_DATE));
        final TagApiDTO tag = new TagApiDTO();
        tag.setKey(TAG_KEY);
        tag.setValues(ImmutableList.of(TAG_VALUE1, TAG_VALUE2));
        costInputApiDTO.setTagFilters(Collections.singletonList(tag));
        costInputApiDTO.setCostGroupBys(Collections.singletonList(CostGroupBy.TAG));

        // ACT
        final GetCloudBilledStatsRequest result = costStatsQueryExecutor.buildGetCloudBilledStatsRequest(
                costInputApiDTO, entityType, Collections.singletonList(ENTITY_OID));

        // ASSERT
        final GetCloudBilledStatsRequest expectedResult = expectedResultBuilder
                .setStartDate(START_DATE)
                .setEndDate(END_DATE)
                .addTagFilter(TagFilter.newBuilder()
                        .setTagKey(TAG_KEY)
                        .addTagValue(TAG_VALUE1)
                        .addTagValue(TAG_VALUE2)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();
        assertEquals(expectedResult, result);
    }

    private static GetCloudBilledStatsResponse buildGetCloudBilledStatsResponse() {
        final GetCloudBilledStatsResponse.Builder responseBuilder = GetCloudBilledStatsResponse.newBuilder();
        final CostStatsSnapshot.Builder costStatsSnapshot1 = CostStatsSnapshot.newBuilder();
        costStatsSnapshot1.setSnapshotDate(1636347600000L);
        final StatRecord.Builder statRecord1 = StatRecord.newBuilder();
        statRecord1.addTag(TagKeyValuePair.newBuilder().setKey(TAG_KEY).setValue(TAG_VALUE1).build());
        statRecord1.setValue(StatValue.newBuilder().setAvg(1).build());
        costStatsSnapshot1.addStatRecords(statRecord1);
        final CostStatsSnapshot.Builder costStatsSnapshot2 = CostStatsSnapshot.newBuilder();
        costStatsSnapshot2.setSnapshotDate(1637094013695L);
        final StatRecord.Builder statRecord2 = StatRecord.newBuilder();
        statRecord2.addTag(TagKeyValuePair.newBuilder().setKey(TAG_KEY).setValue(TAG_VALUE2).build());
        statRecord2.setValue(StatValue.newBuilder().setAvg(1).build());
        costStatsSnapshot2.addStatRecords(statRecord1);
        responseBuilder.addBilledStatRecord(costStatsSnapshot1);
        responseBuilder.addBilledStatRecord(costStatsSnapshot2);
        return responseBuilder.build();
    }

    private static CostInputApiDTO createCostInputApiDTO(@Nonnull List<TagApiDTO> tags) {
        final CostInputApiDTO costInputApiDTO = new CostInputApiDTO();
        costInputApiDTO.setStartDate("1636347600000");
        costInputApiDTO.setEndDate("1637094013695");
        costInputApiDTO.setCostGroupBys(Lists.newArrayList(CostGroupBy.TAG));
        costInputApiDTO.setTagFilters(tags);
        return costInputApiDTO;
    }

    private static TagApiDTO createTagApiDTO() {
        final TagApiDTO tagApiDTO = new TagApiDTO();
        tagApiDTO.setKey(TAG_KEY);
        tagApiDTO.setValues(Lists.newArrayList(TAG_VALUE1, TAG_VALUE2));
        return tagApiDTO;
    }
}
