package com.vmturbo.extractor.topology.fetcher;

import static com.vmturbo.common.protobuf.cost.Cost.CostCategory.ON_DEMAND_COMPUTE;
import static com.vmturbo.common.protobuf.cost.Cost.CostCategory.ON_DEMAND_LICENSE;
import static com.vmturbo.common.protobuf.cost.Cost.CostCategory.RESERVED_LICENSE;
import static com.vmturbo.common.protobuf.cost.Cost.CostCategory.STORAGE;
import static com.vmturbo.common.protobuf.cost.Cost.CostSource.BUY_RI_DISCOUNT;
import static com.vmturbo.common.protobuf.cost.Cost.CostSource.ON_DEMAND_RATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData.BottomUpCostDataPoint;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for {@link BottomUpCostFetcherFactory}.
 */
public class BottomUpCostFetcherFactoryTest {

    private final CostServiceMole costServiceMole = spy(CostServiceMole.class);
    private final MultiStageTimer timer = mock(MultiStageTimer.class);

    /**
     * Mock tests for gRPC services.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(costServiceMole);

    private BottomUpCostFetcherFactory fetcherFactory;

    /**
     * Set up cost service and fetcher.
     */
    @Before
    public void before() {
        final CostServiceBlockingStub costService = CostServiceGrpc.newBlockingStub(server.getChannel());
        fetcherFactory = new BottomUpCostFetcherFactory(costService);
    }

    /**
     * This pretty much tests nearly everything.
     *
     * <p>It sets up a chunked cost response that includes:</p>
     * <ul>
     *     <li>
     *         A an entity for each combination of category & source, with just a single cost whose
     *         value is a function of the category & source
     *     </li>
     *     <li>
     *         Another entity for each combination of  category & source, with a cost for that combo
     *         and for each of the lexicographically preceding combos
     *     </li>
     * </ul>
     *
     * <p>The test uses the fetcher to fetch this collection of costs and fetch them by entity. It then
     * checks that each entity has data points for all the expected category/source combos, as well
     * as per-category totals for all categories that appear, and a grand total cost as well.</p>
     */
    @Test
    public void testThatFetchedCostDataIsCorrect() {
        prepCostResponse();
        final BottomUpCostData entityCosts = fetcherFactory.newCurrentCostFetcher(timer, 1L, trash()).fetch();

        // make sure we've got the right snapshot
        assertThat(entityCosts.getSnapshotTime(), is(1L));

        // total number of category/cost combos, based on sizes of enum classes
        int nCombo = Cost.CostCategory.values().length * Cost.CostSource.values().length;
        // we should have double that number of entities
        assertThat(entityCosts.size(), is(2 * nCombo));
        assertThat(entityCosts.isEmpty(), is(false));

        // our entity oids should run from 1 through to nCombo
        Long[] oids = entityCosts.getEntityOids().toArray(new Long[]{});
        final Long[] expectedOids = LongStream.rangeClosed(1, 2 * nCombo).boxed().toArray(Long[]::new);
        assertThat(oids, is(arrayContainingInAnyOrder(expectedOids)));

        for (int i = 1; i <= nCombo; i++) {
            // an entity in the first half always has a single cost record
            assertThat(entityCosts.getEntityCosts(i).orElse(Collections.emptyList()).size(), is(1));
            // the nth entity in the second half has n cost records
            assertThat(entityCosts.getEntityCosts(nCombo + i).orElse(Collections.emptyList()).size(),
                    is(i));
        }
        // now check all the data points available for each entity
        for (int i = 1; i <= entityCosts.size(); i++) {
            checkDataPoints(i, entityCosts.getEntityCostDataPoints(i));
        }
    }

    /**
     * Test that the fetcher behaves correctly when cost service request fails.
     */
    @Test
    public void testCostServiceErrorYieldsNull() {
        doThrow(new StatusRuntimeException(Status.INTERNAL))
                .when(costServiceMole).getCloudCostStats(any(GetCloudCostStatsRequest.class));
        final BottomUpCostData entityCosts = fetcherFactory.newCurrentCostFetcher(timer, 1L, trash()).fetch();
        assertThat(entityCosts, is(nullValue()));
    }

    /**
     * Test that projected costs are fetched correctly.
     */
    @Test
    public void testFetchProjectedCosts() {
        final List<GetCloudCostStatsResponse> responses = Collections.singletonList(
                GetCloudCostStatsResponse.newBuilder()
                        .addCloudStatRecord(CloudCostStatRecord.newBuilder()
                                .setIsProjected(true)
                                .setSnapshotDate(9527)
                                .addStatRecords(createStatRecord(
                                        ON_DEMAND_COMPUTE,
                                        ON_DEMAND_RATE, 1.2f)
                                        .toBuilder().setAssociatedEntityId(111)))
                        .build());
        doReturn(responses).when(costServiceMole).getCloudCostStats(any(GetCloudCostStatsRequest.class));

        final BottomUpCostData entityCosts = fetcherFactory.newProjectedCostFetcher(timer, trash()).fetch();

        assertThat(entityCosts.getSnapshotTime(), is(9527L));
        assertThat(entityCosts.size(), is(1));
        assertThat(entityCosts.getEntityCosts(111).get().get(0).getValues().getTotal(), is(1.2f));
    }

    /**
     * Test that getting on demand cost and getting on demand rate are correct.
     */
    @Test
    public void testGetOnDemandCosts() {
        final List<GetCloudCostStatsResponse> responses = Collections.singletonList(
                GetCloudCostStatsResponse.newBuilder()
                        .addCloudStatRecord(CloudCostStatRecord.newBuilder()
                                .setSnapshotDate(1234)
                                // vm
                                .addStatRecords(createStatRecord(111, ON_DEMAND_COMPUTE, ON_DEMAND_RATE, 3f))
                                .addStatRecords(createStatRecord(111, ON_DEMAND_LICENSE, ON_DEMAND_RATE, 2f))
                                .addStatRecords(createStatRecord(111, RESERVED_LICENSE, ON_DEMAND_RATE, 1f))
                                .addStatRecords(createStatRecord(111, ON_DEMAND_COMPUTE, BUY_RI_DISCOUNT, -1f))
                                .addStatRecords(createStatRecord(111, STORAGE, ON_DEMAND_RATE, 2f))
                                // volume
                                .addStatRecords(createStatRecord(112, STORAGE, ON_DEMAND_RATE, 1.2f))
                        ).build());
        doReturn(responses).when(costServiceMole).getCloudCostStats(any(GetCloudCostStatsRequest.class));

        final BottomUpCostData entityCosts = fetcherFactory.newCurrentCostFetcher(timer, 1L, trash()).fetch();

        assertThat(entityCosts.size(), is(2));
        // ON_DEMAND_COMPUTE + ON_DEMAND_LICENSE + RESERVED_LICENSE (BUY_RI_DISCOUNT not included)
        assertThat(entityCosts.getOnDemandCost(111, EntityType.VIRTUAL_MACHINE_VALUE).get(), is(6f));
        // ON_DEMAND_COMPUTE + ON_DEMAND_LICENSE
        assertThat(entityCosts.getOnDemandRate(111, EntityType.VIRTUAL_MACHINE_VALUE).get(), is(5f));
        // STORAGE
        assertThat(entityCosts.getOnDemandCost(112, EntityType.VIRTUAL_VOLUME_VALUE).get(), is(1.2f));
        assertThat(entityCosts.getOnDemandRate(112, EntityType.VIRTUAL_VOLUME_VALUE).get(), is(1.2f));
    }

    /**
     * A comparator for ordering data points. For normal data points, we sort based on natural
     * ordering of category and then by normal ordering of source. Category totals come last among
     * data points of the same category, and the grand total comes at the very end.
     */
    private final Comparator<BottomUpCostDataPoint> datapointComparator =
            Comparator.comparing(BottomUpCostDataPoint::getCategory,
                    (a, b) -> a == CostCategory.TOTAL ? b == CostCategory.TOTAL ? 0 : 1
                            : b == CostCategory.TOTAL ? -1
                            : a.compareTo(b))
                    .thenComparing(BottomUpCostDataPoint::getSource,
                            (a, b) -> a == CostSource.TOTAL ? b == CostSource.TOTAL ? 0 : 1
                                    : b == CostSource.TOTAL ? -1
                                    : a.compareTo(b));


    /**
     * Checks that the data points (category+source+cost) available for the given entity are
     * correct. This will include the base data points that correspond directly to the costs returned
     * from the cost component, as well a per-category total for each category that appears, and a
     * grand total across all the entity's costs.
     *
     * @param oid        entity oid
     * @param dataPoints data points associated with that entity
     */
    private void checkDataPoints(final int oid, final List<BottomUpCostDataPoint> dataPoints) {
        final int nCombo = Cost.CostCategory.values().length * Cost.CostSource.values().length;
        // make sure we have the correct number of data points
        assertThat(dataPoints.size(), is(oid <= nCombo ? 3
                // (# of cost points) + (# of category totals) + # of grand totals
                : (oid - nCombo) + (1 + (oid - 1 - nCombo) / Cost.CostSource.values().length) + 1));
        // sort the data points into a predictable order
        dataPoints.sort(datapointComparator);
        if (oid <= nCombo) {
            // for an entity in the first half of the results, we just a normal data point,
            // a category total, and a grand total
            checkNormalDataPoint(oid - 1, dataPoints.get(0));
            checkCatTotalDataPoint(oid - 1, dataPoints.get(1), getCostForCombo(oid - 1));
            checkTotalDataPoint(dataPoints.get(2), getCostForCombo(oid - 1));
        } else {
            // for an entity in the second half we have several normal data points plus totals
            int dpNo = 0;
            float catCost = 0;
            float totCost = 0;
            // loop through the expected category/source combos
            for (int j = 1; j <= oid - nCombo; j++) {
                // our next data point should always be a normal data point for that combo
                checkNormalDataPoint(j - 1, dataPoints.get(dpNo++));
                catCost += getCostForCombo(j - 1);
                totCost += getCostForCombo(j - 1);
                // and then, if either this is our last combo, or if it's the end of a category,
                // we should see a category total data point
                if (j == oid - nCombo || j % Cost.CostSource.values().length == 0) {
                    checkCatTotalDataPoint(j - 1, dataPoints.get(dpNo++), catCost);
                    catCost = 0;
                }
            }
            // and finally we should have a grand total data point
            checkTotalDataPoint(dataPoints.get(dpNo), totCost);
        }
    }

    private void checkNormalDataPoint(final int comboNo, final BottomUpCostDataPoint dataPoint) {
        assertThat("Wrong category", dataPoint.getCategory(), is(getCatForCombo(comboNo)));
        assertThat("Wrong source", dataPoint.getSource(), is(getSrcForCombo(comboNo)));
        assertThat("Wrong cost", dataPoint.getCost(), is(getCostForCombo(comboNo)));
    }

    private void checkCatTotalDataPoint(final int comboNo, final BottomUpCostDataPoint dataPoint, float total) {
        assertThat("Wrong category", dataPoint.getCategory(), is(getCatForCombo(comboNo)));
        assertThat("Wrong source", dataPoint.getSource(), is(CostSource.TOTAL));
        assertThat("Wrong cost", dataPoint.getCost(), is(total));
    }

    private void checkTotalDataPoint(final BottomUpCostDataPoint dataPoint, float total) {
        assertThat("Wrong category", dataPoint.getCategory(), is(CostCategory.TOTAL));
        assertThat("Wrong source", dataPoint.getSource(), is(CostSource.TOTAL));
        assertThat("Wrong cost", dataPoint.getCost(), is(total));
    }

    private CostCategory getCatForCombo(final int comboNo) {
        final int catOrd = comboNo / Cost.CostSource.values().length;
        return CostCategory.valueOf(Cost.CostCategory.values()[catOrd].name());
    }

    private CostSource getSrcForCombo(final int comboNo) {
        final int srcOrd = comboNo % Cost.CostSource.values().length;
        return CostSource.valueOf(Cost.CostSource.values()[srcOrd].name());
    }

    private float getCostForCombo(final int comboNo) {
        final int catOrd = comboNo / Cost.CostSource.values().length;
        final int srcOrd = comboNo % Cost.CostSource.values().length;
        final int sign = catOrd % 2 == 1 && srcOrd % 2 == 1 ? -1 : 1;
        return sign * (catOrd * Cost.CostSource.values().length + srcOrd + 1);
    }

    private void prepCostResponse() {
        // create a collection of stat records, one for each combination of category and source
        // cost values are computed from category and source enum ordinals.
        // These records are incomplete (no oid).
        List<StatRecord> partialRecs = new ArrayList<>();
        for (final Cost.CostCategory category : Cost.CostCategory.values()) {
            for (final Cost.CostSource source : Cost.CostSource.values()) {
                int cat = category.ordinal();
                int src = source.ordinal();
                // if both ordinals are odd, this is a negated cost
                int sign = cat % 2 == 1 && src % 2 == 1 ? -1 : 1;
                float cost = sign * (cat * Cost.CostSource.values().length + src + 1);
                partialRecs.add(createStatRecord(category, source, cost));
            }
        }
        // now we'll create an entity for each combo in isolation, and then an entity for each
        // combo including that combo and all prior combos in the stat record list
        List<StatRecord> records = new ArrayList<>();
        for (int i = 0; i < partialRecs.size(); i++) {
            int oid = i + 1;
            records.add(partialRecs.get(i).toBuilder().setAssociatedEntityId(oid).build());
        }
        for (int i = 0; i < partialRecs.size(); i++) {
            int oid = i + partialRecs.size() + 1;
            for (int j = 0; j < i + 1; j++) {
                records.add(partialRecs.get(j).toBuilder().setAssociatedEntityId(oid).build());
            }
        }
        // now randomize the data and split it into multiple responses
        Collections.shuffle(records);
        final List<GetCloudCostStatsResponse> responses = Lists.partition(records, 10).stream()
                .map(chunk -> CloudCostStatRecord.newBuilder()
                        .setSnapshotDate(1L)
                        .addAllStatRecords(chunk)
                        .build())
                .map(ccsr -> GetCloudCostStatsResponse.newBuilder()
                        .addAllCloudStatRecord(Collections.singletonList(ccsr))
                        .build())
                .collect(Collectors.toList());
        doReturn(responses).when(costServiceMole).getCloudCostStats(any(GetCloudCostStatsRequest.class));
    }

    private StatRecord createStatRecord(Cost.CostCategory category, Cost.CostSource source, float cost) {
        return createStatRecord(0, category, source, cost);
    }

    private StatRecord createStatRecord(long associatedEntityId, Cost.CostCategory category, Cost.CostSource source, float cost) {
        return StatRecord.newBuilder()
                .setAssociatedEntityId(associatedEntityId)
                .setCategory(category)
                .setCostSource(source)
                .setValues(StatValue.newBuilder()
                        .setAvg(cost).setMin(cost).setMax(cost).setTotal(cost)
                        .build())
                .build();
    }

    private static <T> Consumer<T> trash() {
        return t -> {
        };
    }
}
