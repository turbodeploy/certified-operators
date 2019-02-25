package com.vmturbo.api.component.external.api.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceImplBase;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.TagValuesDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.dto.InputField;

/**
 * Tests for {@link EntitiesService}.
 */
public class EntitiesServiceTest {
    // service under test
    private EntitiesService service;

    // mocked topology processor service
    private final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    // data objects
    private TargetInfo targetInfo;
    private ProbeInfo probeInfo;

    // mocked gRPC services
    private final EntitySeverityServiceImplBase entitySeverityService =
            new EntitySeverityServiceImplBase() {};
    private final ActionsServiceImplBase actionsService = new ActionsServiceImplBase() {};
    private final MockSearchService searchService = new MockSearchService();
    private final GroupServiceImplBase groupService = new GroupServiceImplBase() {};

    // gRPC servers
    @Rule
    public final GrpcTestServer entitySeverityServer = GrpcTestServer.newServer(entitySeverityService);
    @Rule
    public final GrpcTestServer actionsServer = GrpcTestServer.newServer(actionsService);
    @Rule
    public final GrpcTestServer groupsServer = GrpcTestServer.newServer(groupService);
    @Rule
    public final GrpcTestServer searchServer = GrpcTestServer.newServer(searchService);

    // a sample topology ST -> PM -> VM
    private static final long contextId = 777777L;
    private static final long targetId = 7L;
    private static final String targetDisplayName = "target";
    private static final long probeId = 70L;
    private static final String probeType = "probe";
    private static final long vmId = 1L;
    private static final String vmDisplayName = "vm";
    private static final EntityState vmState = EntityState.POWERED_OFF;
    private static final String tagKey = "tagKey";
    private static final List<String> tagValues = ImmutableList.of("tagValue1", "tagValue2");
    private static final long pmId = 2L;
    private static final String pmDisplayName = "pm";
    private static final EntityState pmState = EntityState.POWERED_ON;
    private static final long stId = 3L;
    private static final String stDisplayName = "st";
    private static final EntityState stState = EntityState.POWERED_ON;
    private static final long nonExistentId = 999L;
    private static final TopologyEntityDTO vm =
        TopologyEntityDTO.newBuilder()
            .setOid(vmId)
            .setDisplayName(vmDisplayName)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(vmState)
            .setOrigin(
                Origin.newBuilder()
                    .setDiscoveryOrigin(
                        DiscoveryOrigin.newBuilder()
                            .addDiscoveringTargetIds(targetId)
                            .build())
                    .build())
            .putTags(tagKey, TagValuesDTO.newBuilder().addAllValues(tagValues).build())
            .build();
    private static final TopologyEntityDTO pm =
        TopologyEntityDTO.newBuilder()
            .setOid(pmId)
            .setDisplayName(pmDisplayName)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setEntityState(pmState)
            .setOrigin(
                Origin.newBuilder()
                    .setDiscoveryOrigin(
                        DiscoveryOrigin.newBuilder()
                            .addDiscoveringTargetIds(targetId)
                            .build())
                    .build())
            .build();
    private static final TopologyEntityDTO st =
        TopologyEntityDTO.newBuilder()
            .setOid(stId)
            .setDisplayName(stDisplayName)
            .setEntityType(EntityType.STORAGE_VALUE)
            .setEntityState(stState)
            .setOrigin(
                Origin.newBuilder()
                    .setDiscoveryOrigin(
                        DiscoveryOrigin.newBuilder()
                            .addDiscoveringTargetIds(targetId)
                            .build())
                    .build())
            .build();
    private static ImmutableMap<Long, TopologyEntityDTO> topology =
        ImmutableMap.of(vmId, vm, pmId, pm, stId, st);
    private static ImmutableMap<Long, Set<TopologyEntityDTO>> producesRelation =
        ImmutableMap.of(
            vmId, ImmutableSet.of(),
            pmId, ImmutableSet.of(vm),
            stId, ImmutableSet.of(pm));
    private static ImmutableMap<Long, Set<TopologyEntityDTO>> consumesRelation =
        ImmutableMap.of(
            vmId, ImmutableSet.of(pm),
            pmId, ImmutableSet.of(st),
            stId, ImmutableSet.of());

    /**
     * Set up a mock topology processor server and a {@link ProbesService} client and connects them.
     *
     * @throws Exception should not happen.
     */
    @Before
    public void setUp() throws Exception {
        // mock target and probe info
        final AccountValue accountValue =
            new InputField("nameOrAddress", targetDisplayName, Optional.empty());
        targetInfo = mock(TargetInfo.class);
        probeInfo = mock(ProbeInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getAccountData()).thenReturn(Collections.singleton(accountValue));
        when(probeInfo.getId()).thenReturn(probeId);
        when(probeInfo.getType()).thenReturn(probeType);

        // Create service
        service =
            new EntitiesService(
                ActionsServiceGrpc.newBlockingStub(actionsServer.getChannel()),
                mock(ActionSpecMapper.class),
                contextId,
                mock(SupplyChainFetcherFactory.class),
                mock(PaginationMapper.class),
                SearchServiceGrpc.newBlockingStub(searchServer.getChannel()),
                GroupServiceGrpc.newBlockingStub(groupsServer.getChannel()),
                mock(EntityAspectMapper.class),
                topologyProcessor,
                EntitySeverityServiceGrpc.newBlockingStub(entitySeverityServer.getChannel()),
                mock(StatsService.class));
    }

    /**
     * Tests the normal behavior of the {@link EntitiesService#getEntityByUuid(String, boolean)}
     * method, without aspects.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetEntityByUuid() throws Exception {
        // add target and probe
        when(topologyProcessor.getTarget(Matchers.eq(targetId))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(probeId))).thenReturn(probeInfo);

        // call service
        final ServiceEntityApiDTO result = service.getEntityByUuid(Long.toString(pmId), false);

        // check basic information
        Assert.assertEquals(Long.toString(pmId), result.getUuid());
        Assert.assertEquals(pmDisplayName, result.getDisplayName());
        Assert.assertEquals(
            EntityType.PHYSICAL_MACHINE_VALUE, ServiceEntityMapper.fromUIEntityType(result.getClassName()));
        Assert.assertEquals(pmState, UIEntityState.fromString(result.getState()).toEntityState());

        // check target information
        final TargetApiDTO resultTargetInfo = result.getDiscoveredBy();
        Assert.assertEquals(targetId, (long)Long.valueOf(resultTargetInfo.getUuid()));
        Assert.assertEquals(targetDisplayName, resultTargetInfo.getDisplayName());
        Assert.assertEquals(probeType, resultTargetInfo.getType());

        // check providers and consumers
        final List<BaseApiDTO> providers = result.getProviders();
        Assert.assertEquals(1, providers.size());
        Assert.assertEquals(stId, (long)Long.valueOf(providers.get(0).getUuid()));
        Assert.assertEquals(stDisplayName, providers.get(0).getDisplayName());
        Assert.assertEquals(UIEntityType.STORAGE.getValue(), providers.get(0).getClassName());
        final List<BaseApiDTO> consumers = result.getConsumers();
        Assert.assertEquals(1, consumers.size());
        Assert.assertEquals(vmId, (long)Long.valueOf(consumers.get(0).getUuid()));
        Assert.assertEquals(vmDisplayName, consumers.get(0).getDisplayName());
        Assert.assertEquals(UIEntityType.VIRTUAL_MACHINE.getValue(), consumers.get(0).getClassName());

        // check tags
        Assert.assertEquals(0, result.getTags().size());
    }

    /**
     * Searching for a non-existent entity should cause an {@link StatusRuntimeException}.
     *
     * @throws Exception expected: for entity not found.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetEntityByUuidNonExistent() throws Exception {
        // add target and probe
        when(topologyProcessor.getTarget(Matchers.eq(targetId))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(probeId))).thenReturn(probeInfo);

        // call service and fail
        service.getEntityByUuid(Long.toString(nonExistentId), false);
    }

    /**
     * When calls to topology processor fail, the rest of the data should be fetched successfully
     * and the discovered-by field of the result should be null.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetEntityByUuidMissingTarget() throws Exception {
        // error while fetching the target
        when(topologyProcessor.getTarget(Matchers.eq(targetId)))
            .thenThrow(new TopologyProcessorException("boom"));

        // call service
        final ServiceEntityApiDTO result = service.getEntityByUuid(Long.toString(stId), false);

        // check basic information
        Assert.assertEquals(Long.toString(stId), result.getUuid());
        Assert.assertEquals(stDisplayName, result.getDisplayName());
        Assert.assertEquals(
            EntityType.STORAGE_VALUE, ServiceEntityMapper.fromUIEntityType(result.getClassName()));
        Assert.assertEquals(stState, UIEntityState.fromString(result.getState()).toEntityState());

        // there should be no target information; not even an empty record
        Assert.assertNull(result.getDiscoveredBy());

        // check providers and consumers
        final List<BaseApiDTO> providers = result.getProviders();
        Assert.assertEquals(0, providers.size());
        final List<BaseApiDTO> consumers = result.getConsumers();
        Assert.assertEquals(1, consumers.size());
        Assert.assertEquals(pmId, (long)Long.valueOf(consumers.get(0).getUuid()));
        Assert.assertEquals(pmDisplayName, consumers.get(0).getDisplayName());
        Assert.assertEquals(UIEntityType.PHYSICAL_MACHINE.getValue(), consumers.get(0).getClassName());

        // check tags
        Assert.assertEquals(0, result.getTags().size());
    }

    /**
     * When calls to fetch providers or consumers fail, the rest of the data
     * should be fetched successfully and the providers or consumers field resp.
     * of the result should be null.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetEntityByUuidMissingProducers() throws Exception {
        // add target and probe
        when(topologyProcessor.getTarget(Matchers.eq(targetId))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(probeId))).thenReturn(probeInfo);

        // pretend that traversal queries will not work
        searchService.breakTraversals();

        // call service
        final ServiceEntityApiDTO result = service.getEntityByUuid(Long.toString(vmId), false);

        // check basic information
        Assert.assertEquals(Long.toString(vmId), result.getUuid());
        Assert.assertEquals(vmDisplayName, result.getDisplayName());
        Assert.assertEquals(
                EntityType.VIRTUAL_MACHINE_VALUE, ServiceEntityMapper.fromUIEntityType(result.getClassName()));
        Assert.assertEquals(vmState, UIEntityState.fromString(result.getState()).toEntityState());

        // check target information
        final TargetApiDTO resultTargetInfo = result.getDiscoveredBy();
        Assert.assertEquals(targetId, (long)Long.valueOf(resultTargetInfo.getUuid()));
        Assert.assertEquals(targetDisplayName, resultTargetInfo.getDisplayName());
        Assert.assertEquals(probeType, resultTargetInfo.getType());

        // there should no provider or consumer information; not even empty lists
        Assert.assertNull(result.getConsumers());
        Assert.assertNull(result.getProviders());

        // check tags
        Assert.assertEquals(1, result.getTags().size());
        Assert.assertArrayEquals(tagValues.toArray(), result.getTags().get(tagKey).toArray());
    }

    /**
     * Get tags by entity id should work as expected, if tags exist.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetTags() throws Exception {
        // add target and probe
        when(topologyProcessor.getTarget(Matchers.eq(targetId))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(probeId))).thenReturn(probeInfo);

        // call service
        final List<TagApiDTO> result = service.getTagsByEntityUuid(Long.toString(vmId));

        // check tags
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(tagKey, result.get(0).getKey());
        Assert.assertArrayEquals(tagValues.toArray(), result.get(0).getValues().toArray());
    }

    /**
     * Get tags by entity id should work as expected, if tags don't exist.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetEmptyTags() throws Exception {
        // add target and probe
        when(topologyProcessor.getTarget(Matchers.eq(targetId))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(probeId))).thenReturn(probeInfo);

        // call service
        final List<TagApiDTO> result = service.getTagsByEntityUuid(Long.toString(pmId));

        // check tags
        Assert.assertEquals(0, result.size());
    }

    private static class MockSearchService extends SearchServiceImplBase {
        /**
         * When this flag is true, the mock search service fails to answer traversals.
         */
        private boolean brokenTraversals = false;

        /**
         * Break traversal queries.
         */
        public void breakTraversals() {
            brokenTraversals = true;
        }

        @Override
        public void searchTopologyEntityDTOs(
                SearchTopologyEntityDTOsRequest request,
                StreamObserver<SearchTopologyEntityDTOsResponse> response) {
            if (request.getEntityOidCount() != 0) {
                // this request is looking for specific entities
                final SearchTopologyEntityDTOsResponse.Builder resultBuilder =
                    SearchTopologyEntityDTOsResponse.newBuilder();
                request.getEntityOidList().stream()
                    .map(topology::get)
                    .forEach(resultBuilder::addTopologyEntityDtos);
                response.onNext(resultBuilder.build());
                response.onCompleted();
                return;
            }

            if (request.getSearchParametersCount() != 0) {
                // this request is a general search request
                // we assume that this is a "neighbors" request, i.e.,
                // it requests all the producers or consumers of a specific entity
                if (brokenTraversals) {
                    response.onError(new OperationFailedException("traversal query failed"));
                    return;
                }

                final long startingEntityId =
                    Long.valueOf(
                        request
                            .getSearchParameters(0)
                            .getStartingFilter()
                            .getStringFilter()
                            .getStringPropertyRegex());
                final TraversalDirection traversalDirection =
                    request
                        .getSearchParameters(0)
                        .getSearchFilter(0)
                        .getTraversalFilter()
                        .getTraversalDirection();
                final Set<TopologyEntityDTO> responseSet =
                    (traversalDirection == TraversalDirection.CONSUMES ? consumesRelation : producesRelation)
                        .get(startingEntityId);
                response.onNext(
                    SearchTopologyEntityDTOsResponse.newBuilder()
                        .addAllTopologyEntityDtos(responseSet)
                        .build());
                response.onCompleted();
                return;
            }

            // no other requests are supported by this mock service
            response.onError(new OperationFailedException("not supported"));
        }
    }
}
