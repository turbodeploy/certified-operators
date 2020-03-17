package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class tests the functionality in CloudPlanNumEntitiesByTierSubQuery.java.
 *
 */
//@RunWith(MockitoJUnitRunner.class)
public class CloudPlanNumEntitiesByTierSubQueryTest {
    private static final String STORAGE_TIER_ST = "ST";
    private static final String STORAGE_TIER_IO = "IO";
    private static final Long BEFORE_TIME = 1111111111L;
    private static final Long AFTER_TIME = 1197511111L;
    private static final Set<Long> SCOPE = Collections.singleton(12345L);
    private static final Set<Long> VOLUMES_IDS = Stream.of(10001L, 10002L).collect(Collectors.toSet());
    private static final Map<String, Set<Long>> RELATED_ENTITIES = createRelatedEntitiesMap();
    private static final Stream<ApiPartialEntity> SOURCE_VOLUMES_ENTITIES = createVolumesEntities(1, 2);
    private static final Stream<MinimalEntity> SOURCE_TIERS_ENTITIES = createTiersEntities(2, STORAGE_TIER_ST);
    private static final Stream<ApiPartialEntity> PROJECTED_VOLUMES_ENTITIES = createVolumesEntities(1, 3);
    private static final Stream<MinimalEntity> PROJECTED_TIERS_ENTITIES = createTiersEntities(3, STORAGE_TIER_IO);
    private static final Map<Long, List<String>> TIME_TO_TYPES = createFiltersTypesMap();
    private static final Map<Long, List<String>> TIME_TO_VALUES = createFiltersValuesMap();

    @Mock
    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    CloudPlanNumEntitiesByTierSubQuery query;

    @Mock
    private RepositoryApi repositoryApi;

    /**
     * Initializations to run before the tests in this class.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        query = new CloudPlanNumEntitiesByTierSubQuery(repositoryApi, supplyChainFetcherFactory);
    }

    private static Map<String, Set<Long>> createRelatedEntitiesMap() {
        Map<String, Set<Long>> result = new HashMap<>();
        result.put(ApiEntityType.VIRTUAL_VOLUME.apiStr(), VOLUMES_IDS);
        return Collections.unmodifiableMap(result);
    }

    private static Stream<ApiPartialEntity> createVolumesEntities(long oid, long tierOid) {
        final RelatedEntity storageTier = RelatedEntity.newBuilder().setOid(tierOid)
                        .setEntityType(EntityType.STORAGE_TIER_VALUE).build();
        final ApiPartialEntity virtualVolume = ApiPartialEntity.newBuilder().setOid(oid)
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE).addConnectedTo(storageTier)
                        .build();
        return Stream.of(virtualVolume);
    }

    private static Stream<MinimalEntity> createTiersEntities(long oid, String displayName) {
        final MinimalEntity tier = MinimalEntity.newBuilder().setOid(oid)
                        .setEntityType(EntityType.STORAGE_TIER_VALUE).setDisplayName(displayName)
                        .build();
        return Stream.of(tier);
    }

    private static Map<Long, List<String>> createFiltersTypesMap() {
        final Map<Long, List<String>> result = new HashMap<>();
        result.put(BEFORE_TIME, Arrays.asList("tier", "resultsType"));
        result.put(AFTER_TIME, Collections.singletonList("tier"));
        return Collections.unmodifiableMap(result);
    }

    private static Map<Long, List<String>> createFiltersValuesMap() {
        final Map<Long, List<String>> result = new HashMap<>();
        result.put(BEFORE_TIME, Arrays.asList(STORAGE_TIER_ST, "beforePlan"));
        result.put(AFTER_TIME, Collections.singletonList(STORAGE_TIER_IO));
        return Collections.unmodifiableMap(result);
    }

    /**
     * The Map, CloudNumPlanEntitiesByTierSubQuery.ENTITY_TYPE_TO_GET_TIER_FUNCTION, is used for
     * mapping the number of providers by tier type.
     *
     * <p>This tests tests the lambda function that gets the tier id and the number of entities (providers) by tier type,
     * from a given TopologyEntityDTO.
     */
    @Test
    public void testEntityTypeToGetTierFunction() {
        final RelatedEntity storage = RelatedEntity.newBuilder()
                        .setEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setOid(7777L)
                        .build();
        final TopologyEntityDTO topologyEntityVM = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(77777L)
                        .build();
        final ApiPartialEntity virtualVolume1 = ApiPartialEntity.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .setOid(777777L)
                        .build();
        final ApiPartialEntity virtualVolume2 = ApiPartialEntity.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .addConnectedTo(storage)
                        .setOid(7777777L)
                        .build();

        // Add and retrieve entries from CloudNumPlanEntitiesByTierSubQuery.ENTITY_TYPE_TO_GET_TIER_FUNCTION
        // Test that the assertions pass without exceptions whether the provider type is present or
        // not.  Test the lambda function that computes the number of entities by tier type.
        Map<Long, ApiPartialEntity> entities = new HashMap<>();
        String volumeEntityType = ApiEntityType.VIRTUAL_VOLUME.apiStr();
        entities.put(7777777L, virtualVolume1);
        Map<Optional<Long>, Long> tierIdToNumEntities = entities.values().stream()
                        .collect(Collectors.groupingBy(CloudPlanNumEntitiesByTierSubQuery.ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(volumeEntityType), Collectors.counting()));

        // When there are no providers for VIRTUAL_VOLUME, the function to map tier id to number of providers returns 0.
        assertEquals(1, tierIdToNumEntities.size());
        assertEquals(null, tierIdToNumEntities.get(null));

        tierIdToNumEntities.clear();
        entities.clear();
        entities.put(7777777L, virtualVolume2);
        tierIdToNumEntities = entities.values().stream()
                        .collect(Collectors.groupingBy(CloudPlanNumEntitiesByTierSubQuery.ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(volumeEntityType), Collectors.counting()));
        // When there are is a provider providers for VIRTUAL_VOLUME, the function to map tier id to number of providers returns the count of providers.
        assertEquals(1, tierIdToNumEntities.size());
        assertEquals(Long.valueOf(1), tierIdToNumEntities.get(Optional.of(7777L)));
    }

    /**
     * Tests get aggregated stats method for the CloudPlanNumEntitiesByTierSubQuery.
     *
     * @throws OperationFailedException If anything goes wrong during getting stats.
     */
    @Test
    public void testGetAggregateStats() throws OperationFailedException {
        final SupplyChainNodeFetcherBuilder builder = Mockito
                        .mock(SupplyChainNodeFetcherBuilder.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(builder.addSeedUuids(Matchers.anyCollection())
                        .entityTypes(Matchers.anyList())
                        .environmentType(null)
                        .fetch().values().stream().collect(Matchers.any()))
                        .thenReturn(RELATED_ENTITIES);
        Mockito.when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(builder);
        final StatApiInputDTO requestedStats =
                                             new StatApiInputDTO(StringConstants.NUM_VIRTUAL_DISKS,
                                                                 null, null, null);
        final PlanInstance planInstance = PlanInstance.newBuilder()
            .setStartTime(BEFORE_TIME)
            .setEndTime(AFTER_TIME)
            .setPlanId(0L).setStatus(PlanStatus.SUCCEEDED).build();
        final StatsQueryContext context = mock(StatsQueryContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getPlanInstance()).thenReturn(Optional.of(planInstance));
        Mockito.when(context.getInputScope().oid()).thenReturn(22222L);
        Mockito.when(context.getQueryScope().getExpandedOids()).thenReturn(SCOPE);
        MultiEntityRequest request = Mockito.mock(MultiEntityRequest.class);
        Mockito.when(repositoryApi.entitiesRequest(Matchers.anySet())).thenReturn(request);
        Mockito.when(request.contextId(22222L)).thenReturn(request);
        Mockito.when(request.projectedTopology()).thenReturn(request);
        Mockito.when(request.getEntities()).thenReturn(SOURCE_VOLUMES_ENTITIES)
                        .thenReturn(PROJECTED_VOLUMES_ENTITIES);
        Mockito.when(request.getMinimalEntities()).thenReturn(SOURCE_TIERS_ENTITIES)
                        .thenReturn(PROJECTED_TIERS_ENTITIES);
        final List<StatSnapshotApiDTO> result = query
                        .getAggregateStats(Collections.singleton(requestedStats), context);
        checkResult(result, BEFORE_TIME, 2);
        checkResult(result, AFTER_TIME, 1);
    }

    private static void checkResult(List<StatSnapshotApiDTO> result, Long time, int filtersSize) {
        final List<StatApiDTO> statsList = result.stream()
            // We must compare datetimes as Strings because our ISO format lacks millisecond precision,
            // and thus converting back to a long will be lossy and the values may not be equal.
            .filter(statSnapshotApiDTO -> DateTimeUtil.toString(time).equals(statSnapshotApiDTO.getDate()))
            .map(StatSnapshotApiDTO::getStatistics)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        Assert.assertNotNull(statsList);
        final StatApiDTO stats = statsList.get(0);
        List<StatFilterApiDTO> filters = stats.getFilters();
        Assert.assertEquals(filtersSize, filters.size());
        final List<String> types = TIME_TO_TYPES.get(time);
        final List<String> values = TIME_TO_VALUES.get(time);
        for (int i = 0; i < filtersSize; i++) {
            StatFilterApiDTO filter = filters.get(i);
            Assert.assertEquals(types.get(i), filter.getType());
            Assert.assertEquals(values.get(i), filter.getValue());
        }
        Assert.assertEquals(1, stats.getValue().longValue());
    }
}
