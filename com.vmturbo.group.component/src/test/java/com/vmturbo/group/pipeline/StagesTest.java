package com.vmturbo.group.pipeline;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.CloudTypeEnum.CloudType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.group.GroupEnvironment;
import com.vmturbo.group.group.GroupEnvironmentTypeResolver;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.pipeline.Stages.StoreSupplementaryGroupInfoStage;
import com.vmturbo.group.pipeline.Stages.UpdateGroupMembershipCacheStage;
import com.vmturbo.group.service.CachingMemberCalculator;
import com.vmturbo.group.service.CachingMemberCalculator.RegroupingResult;
import com.vmturbo.group.service.MockGroupStore;
import com.vmturbo.group.service.MockTransactionProvider;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Test class for the various stages of a {@link GroupInfoUpdatePipeline}.
 */
public class StagesTest {

    final CachingMemberCalculator memberCache = mock(CachingMemberCalculator.class);
    final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver =
            mock(GroupEnvironmentTypeResolver.class);
    private final GroupSeverityCalculator groupSeverityCalculator =
            mock(GroupSeverityCalculator.class);
    private SearchServiceMole searchServiceMole;
    private GrpcTestServer testServer;
    private MockTransactionProvider transactionProvider;
    private MockGroupStore groupStoreMock;

    /**
     * Sets up environment for tests.
     *
     * @throws IOException on grpc server error.
     */
    @Before
    public void setUp() throws IOException {
        searchServiceMole = Mockito.spy(new SearchServiceMole());
        testServer = GrpcTestServer.newServer(searchServiceMole);
        testServer.start();
        transactionProvider = new MockTransactionProvider();
        groupStoreMock = transactionProvider.getGroupStore();
    }

    /**
     * Tests that during {@link UpdateGroupMembershipCacheStage},
     * {@link CachingMemberCalculator#regroup()} is being executed.
     */
    @Test
    public void testUpdateGroupMembershipCacheStage() {
        // GIVEN
        final UpdateGroupMembershipCacheStage stage =
                new UpdateGroupMembershipCacheStage(memberCache);
        LongOpenHashSet groupIds = new LongOpenHashSet();
        groupIds.add(1L);
        groupIds.add(2L);
        groupIds.add(3L);
        RegroupingResult regroupingResult = mock(RegroupingResult.class);
        when(regroupingResult.isSuccessfull()).thenReturn(true);
        when(regroupingResult.getResolvedGroupsIds()).thenReturn(groupIds);
        when(regroupingResult.getTotalMemberCount()).thenReturn(6L);
        when(regroupingResult.getDistinctEntitiesCount()).thenReturn(4);
        when(regroupingResult.getMemory()).thenReturn(MemoryMeasurer.measure(new Object()));
        when(memberCache.regroup()).thenReturn(regroupingResult);
        // WHEN
        StageResult<LongSet> stageResult = stage.executeStage(null);
        // THEN
        verify(memberCache, times(1)).regroup();
        Assert.assertEquals(Status.Type.SUCCEEDED, stageResult.getStatus().getType());
        Assert.assertEquals(groupIds.size(), stageResult.getResult().size());
    }

    /**
     * Tests that {@link StoreSupplementaryGroupInfoStage} refreshes the data in the database.
     *
     * @throws StoreOperationException on cache error
     */
    @Test
    public void testStoreSupplementaryGroupInfoStage() throws StoreOperationException {
        final SearchServiceBlockingStub searchServiceRpc =
                SearchServiceGrpc.newBlockingStub(testServer.getChannel());
        final StoreSupplementaryGroupInfoStage stage =
                new StoreSupplementaryGroupInfoStage(memberCache, searchServiceRpc,
                        groupEnvironmentTypeResolver, groupSeverityCalculator, groupStoreMock);
        // GIVEN
        final long groupUuid1 = 1;
        final long groupUuid2 = 2;
        final long entityUuid1 = 10;
        final long entityUuid2 = 20;
        final long entityUuid3 = 30;
        final long entityUuid4 = 40;
        final Set<Long> group1entities = new HashSet<>();
        group1entities.add(entityUuid1);
        group1entities.add(entityUuid2);
        final Set<Long> group2entities = new HashSet<>();
        group2entities.add(entityUuid3);
        group2entities.add(entityUuid4);
        final LongOpenHashSet input = new LongOpenHashSet();
        input.add(groupUuid1);
        input.add(groupUuid2);
        final EntityWithOnlyEnvironmentTypeAndTargets entityWithEnv1 =
                createEntityWithOnlyEnvironmentTypeAndTargets(entityUuid1);
        final EntityWithOnlyEnvironmentTypeAndTargets entityWithEnv2 =
                createEntityWithOnlyEnvironmentTypeAndTargets(entityUuid2);
        final EntityWithOnlyEnvironmentTypeAndTargets entityWithEnv3 =
                createEntityWithOnlyEnvironmentTypeAndTargets(entityUuid3);
        final EntityWithOnlyEnvironmentTypeAndTargets entityWithEnv4 =
                createEntityWithOnlyEnvironmentTypeAndTargets(entityUuid4);
        final Set<EntityWithOnlyEnvironmentTypeAndTargets> group1entitiesWithEnvType =
                new HashSet<>();
        group1entitiesWithEnvType.add(entityWithEnv1);
        group1entitiesWithEnvType.add(entityWithEnv2);
        final Set<EntityWithOnlyEnvironmentTypeAndTargets> group2entitiesWithEnvType =
                new HashSet<>();
        group2entitiesWithEnvType.add(entityWithEnv3);
        group2entitiesWithEnvType.add(entityWithEnv4);
        final List<PartialEntity> partialEntities = new ArrayList<>();
        partialEntities.add(createPartialEntityWithOnlyEnvironmentTypeAndTargets(entityWithEnv1));
        partialEntities.add(createPartialEntityWithOnlyEnvironmentTypeAndTargets(entityWithEnv2));
        partialEntities.add(createPartialEntityWithOnlyEnvironmentTypeAndTargets(entityWithEnv3));
        partialEntities.add(createPartialEntityWithOnlyEnvironmentTypeAndTargets(entityWithEnv4));
        final PartialEntityBatch repositoryResult = PartialEntityBatch.newBuilder()
                .addAllEntities(partialEntities)
                .build();
        final List<PartialEntityBatch> repositoryResults = new ArrayList<>();
        repositoryResults.add(repositoryResult);
        when(searchServiceMole.searchEntitiesStream(SearchEntitiesRequest.newBuilder()
                .setSearch(SearchQuery.getDefaultInstance())
                .setReturnType(Type.WITH_ONLY_ENVIRONMENT_TYPE_AND_TARGETS)
                .build())).thenReturn(repositoryResults);
        when(memberCache.getGroupMembers(groupStoreMock, Collections.singleton(groupUuid1), true))
                .thenReturn(group1entities);
        when(groupEnvironmentTypeResolver.getEnvironmentAndCloudTypeForGroup(eq(groupUuid1),
                eq(group1entitiesWithEnvType),
                eq(ArrayListMultimap.create()))).thenReturn(
                        new GroupEnvironment(EnvironmentType.ON_PREM, CloudType.UNKNOWN_CLOUD));
        when(groupSeverityCalculator.calculateSeverity(group1entities)).thenReturn(Severity.NORMAL);
        when(groupEnvironmentTypeResolver.getEnvironmentAndCloudTypeForGroup(eq(groupUuid2),
                eq(group2entitiesWithEnvType),
                eq(ArrayListMultimap.create()))).thenReturn(
                        new GroupEnvironment(EnvironmentType.HYBRID, CloudType.AWS));
        when(groupSeverityCalculator.calculateSeverity(group2entities)).thenReturn(Severity.CRITICAL);
        when(memberCache.getGroupMembers(groupStoreMock, Collections.singleton(groupUuid2), true))
                .thenReturn(group2entities);
        // WHEN
        Status status = stage.passthrough(input);

        // THEN
        ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
        // verify that even though we have multiple groups, there is only one (bulk) update
        verify(groupStoreMock, times(1))
                .updateBulkGroupSupplementaryInfo(captor.capture());
        Assert.assertEquals(Status.success().getType(), status.getType());
        // validate the arguments passed to updateBulkGroupSupplementaryInfo
        Assert.assertEquals(2, captor.getValue().size());
        Iterator<GroupSupplementaryInfo> it = captor.getValue().iterator();
        // group1
        validateGroupSupplementaryInfo(it.next(), groupUuid1, false,
                EnvironmentType.ON_PREM.getNumber(), CloudType.UNKNOWN_CLOUD.getNumber(),
                Severity.NORMAL.getNumber());
        // group2
        validateGroupSupplementaryInfo(it.next(), groupUuid2, false,
                EnvironmentType.HYBRID.getNumber(), CloudType.AWS.getNumber(),
                Severity.CRITICAL.getNumber());
    }

    /**
     * Utility function to validate the values inside a {@link GroupSupplementaryInfo}.
     *
     * @param gsi the {@link GroupSupplementaryInfo} to validate.
     * @param groupId expected group id.
     * @param empty expected emptiness state.
     * @param envType expected environment type.
     * @param cloudType expected cloud type.
     * @param severity expected severity.
     */
    private void validateGroupSupplementaryInfo(GroupSupplementaryInfo gsi, long groupId,
            boolean empty, int envType, int cloudType, int severity) {
        Assert.assertEquals(groupId, gsi.getGroupId().longValue());
        Assert.assertEquals(empty, gsi.getEmpty());
        Assert.assertEquals(envType, gsi.getEnvironmentType().intValue());
        Assert.assertEquals(cloudType, gsi.getCloudType().intValue());
        Assert.assertEquals(severity, gsi.getSeverity().intValue());
    }

    /**
     * Utility function that creates a new {@link EntityWithOnlyEnvironmentTypeAndTargets} with the
     * oid provided and some default values for the rest of the fields.
     *
     * @param oid the entity's oid.
     * @return the new {@link EntityWithOnlyEnvironmentTypeAndTargets} object.
     */
    private EntityWithOnlyEnvironmentTypeAndTargets createEntityWithOnlyEnvironmentTypeAndTargets(
            long oid) {
        return EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(oid)
                .addDiscoveringTargetIds(100)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build();
    }

    /**
     * Utility function that creates a new {@link PartialEntity} containing the
     * {@link EntityWithOnlyEnvironmentTypeAndTargets} provided.
     *
     * @param entity the {@link EntityWithOnlyEnvironmentTypeAndTargets} object.
     * @return the new {@link PartialEntity object}.
     */
    private PartialEntity createPartialEntityWithOnlyEnvironmentTypeAndTargets(
            EntityWithOnlyEnvironmentTypeAndTargets entity) {
        return PartialEntity.newBuilder()
                .setWithOnlyEnvironmentTypeAndTargets(entity)
                .build();
    }
}
