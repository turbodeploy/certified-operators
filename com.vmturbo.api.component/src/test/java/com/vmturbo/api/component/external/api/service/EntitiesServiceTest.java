package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import com.google.common.collect.ImmutableList;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityBatch;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reporting.api.protobuf.ReportingMoles.ReportingServiceMole;
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

    // mocked services and mappers
    private final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);
    private final ActionSpecMapper actionSpecMapper = mock(ActionSpecMapper.class);

    // data objects
    private TargetInfo targetInfo;
    private ProbeInfo probeInfo;

    private final SeverityPopulator severityPopulator = mock(SeverityPopulator.class);
    private final ActionsServiceMole actionsService = spy(new ActionsServiceMole());
    private final SearchServiceMole searchService = spy(new SearchServiceMole());
    private final GroupServiceImplBase groupService = spy(new GroupServiceMole());
    private final StatsHistoryServiceMole historyService = spy(new StatsHistoryServiceMole());
    private final RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private final ReportingServiceMole reportingService = spy(new ReportingServiceMole());
    private final SupplyChainFetcherFactory supplyChainFetcherFactory =
            mock(SupplyChainFetcherFactory.class);

    // gRPC servers
    @Rule
    public final GrpcTestServer grpcServer =
        GrpcTestServer.newServer(
            actionsService, searchService, groupService,
            historyService, reportingService, repositoryService);

    // a sample topology ST -> PM -> VM
    private static final long CONTEXT_ID = 777777L;
    private static final long TARGET_ID = 7L;
    private static final String TARGET_DISPLAY_NAME = "target";
    private static final long PROBE_ID = 70L;
    private static final String PROBE_TYPE = "probe";
    private static final long VM_ID = 1L;
    private static final String VM_DISPLAY_NAME = "VM";
    private static final EntityState VM_STATE = EntityState.POWERED_OFF;
    private static final String TAG_KEY = "TAG_KEY";
    private static final List<String> TAG_VALUES = ImmutableList.of("tagValue1", "tagValue2");
    private static final long PM_ID = 2L;
    private static final String PM_DISPLAY_NAME = "PM";
    private static final EntityState PM_STATE = EntityState.POWERED_ON;
    private static final long ST_ID = 3L;
    private static final String ST_DISPLAY_NAME = "ST";
    private static final EntityState ST_STATE = EntityState.POWERED_ON;
    private static final long NON_EXISTENT_ID = 999L;
    private static final TopologyEntityDTO VM =
        TopologyEntityDTO.newBuilder()
            .setOid(VM_ID)
            .setDisplayName(VM_DISPLAY_NAME)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(VM_STATE)
            .setOrigin(
                Origin.newBuilder()
                    .setDiscoveryOrigin(
                        DiscoveryOrigin.newBuilder()
                            .addDiscoveringTargetIds(TARGET_ID)
                            .build())
                    .build())
            .setTags(
                    Tags.newBuilder()
                            .putTags(
                                    TAG_KEY,
                                    TagValuesDTO.newBuilder().addAllValues(TAG_VALUES).build())
                            .build())
            .build();
    private static final TopologyEntityDTO PM =
        TopologyEntityDTO.newBuilder()
            .setOid(PM_ID)
            .setDisplayName(PM_DISPLAY_NAME)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setEntityState(PM_STATE)
            .setOrigin(
                Origin.newBuilder()
                    .setDiscoveryOrigin(
                        DiscoveryOrigin.newBuilder()
                            .addDiscoveringTargetIds(TARGET_ID)
                            .build())
                    .build())
            .build();
    private static final TopologyEntityDTO ST =
        TopologyEntityDTO.newBuilder()
            .setOid(ST_ID)
            .setDisplayName(ST_DISPLAY_NAME)
            .setEntityType(EntityType.STORAGE_VALUE)
            .setEntityState(ST_STATE)
            .setOrigin(
                Origin.newBuilder()
                    .setDiscoveryOrigin(
                        DiscoveryOrigin.newBuilder()
                            .addDiscoveringTargetIds(TARGET_ID)
                            .build())
                    .build())
            .build();

    /**
     * Set up a mock topology processor server and a {@link ProbesService} client and connects them.
     *
     * @throws Exception should not happen.
     */
    @Before
    public void setUp() throws Exception {
        // mock target and probe info
        final AccountValue accountValue =
            new InputField("nameOrAddress", TARGET_DISPLAY_NAME, Optional.empty());
        targetInfo = mock(TargetInfo.class);
        probeInfo = mock(ProbeInfo.class);
        when(targetInfo.getId()).thenReturn(TARGET_ID);
        when(targetInfo.getProbeId()).thenReturn(PROBE_ID);
        when(targetInfo.getAccountData()).thenReturn(Collections.singleton(accountValue));
        when(probeInfo.getId()).thenReturn(PROBE_ID);
        when(probeInfo.getType()).thenReturn(PROBE_TYPE);

        // create inputs for the service
        final ServiceEntityMapper serviceEntityMapper = new ServiceEntityMapper(topologyProcessor);
        final SearchServiceBlockingStub searchServiceRpc =
            SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final RepositoryServiceBlockingStub repositoryServiceRpc =
            RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final ActionsServiceBlockingStub actionOrchestratorRpcService =
            ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final PaginationMapper paginationMapper = new PaginationMapper();
        final RepositoryApi repositoryApi = new RepositoryApi(
                mock(SeverityPopulator.class),
                repositoryServiceRpc,
                searchServiceRpc,
                serviceEntityMapper,
                CONTEXT_ID);
        final ActionSearchUtil actionSearchUtil =
            new ActionSearchUtil(
                actionOrchestratorRpcService, actionSpecMapper,
                paginationMapper, supplyChainFetcherFactory, CONTEXT_ID);

        // Create service
        service =
            new EntitiesService(
                actionOrchestratorRpcService,
                actionSpecMapper,
                CONTEXT_ID,
                supplyChainFetcherFactory,
                paginationMapper,
                searchServiceRpc,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                mock(EntityAspectMapper.class),
                severityPopulator,
                mock(StatsService.class),
                mock(ActionStatsQueryExecutor.class),
                mock(UuidMapper.class),
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SettingServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                mock(SettingsMapper.class),
                actionSearchUtil,
                repositoryApi);
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
        when(topologyProcessor.getTarget(Matchers.eq(TARGET_ID))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(PROBE_ID))).thenReturn(probeInfo);

        when(repositoryService.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder().addEntityOids(PM_ID).build()))
            .thenReturn(Collections.singletonList(EntityBatch.newBuilder().addEntities(PM).build()));
        when(searchService.searchTopologyEntityDTOs(
                SearchTopologyEntityDTOsRequest.newBuilder()
                    .addSearchParameters(SearchProtoUtil.neighbors(PM_ID, TraversalDirection.PRODUCES))
                    .build()))
            .thenReturn(SearchTopologyEntityDTOsResponse.newBuilder().addTopologyEntityDtos(VM).build());
        when(searchService.searchTopologyEntityDTOs(
                SearchTopologyEntityDTOsRequest.newBuilder()
                    .addSearchParameters(SearchProtoUtil.neighbors(PM_ID, TraversalDirection.CONSUMES))
                    .build()))
            .thenReturn(SearchTopologyEntityDTOsResponse.newBuilder().addTopologyEntityDtos(ST).build());

        // call service
        final ServiceEntityApiDTO result = service.getEntityByUuid(Long.toString(PM_ID), false);

        // check basic information
        Assert.assertEquals(Long.toString(PM_ID), result.getUuid());
        Assert.assertEquals(PM_DISPLAY_NAME, result.getDisplayName());
        Assert.assertEquals(
            EntityType.PHYSICAL_MACHINE_VALUE, UIEntityType.fromString(result.getClassName()).typeNumber());
        Assert.assertEquals(PM_STATE, UIEntityState.fromString(result.getState()).toEntityState());

        // check target information
        final TargetApiDTO resultTargetInfo = result.getDiscoveredBy();
        Assert.assertEquals(TARGET_ID, (long)Long.valueOf(resultTargetInfo.getUuid()));
        Assert.assertEquals(TARGET_DISPLAY_NAME, resultTargetInfo.getDisplayName());
        Assert.assertEquals(PROBE_TYPE, resultTargetInfo.getType());

        // check providers and consumers
        final List<BaseApiDTO> providers = result.getProviders();
        Assert.assertEquals(1, providers.size());
        Assert.assertEquals(ST_ID, (long)Long.valueOf(providers.get(0).getUuid()));
        Assert.assertEquals(ST_DISPLAY_NAME, providers.get(0).getDisplayName());
        Assert.assertEquals(UIEntityType.STORAGE.apiStr(), providers.get(0).getClassName());
        final List<BaseApiDTO> consumers = result.getConsumers();
        Assert.assertEquals(1, consumers.size());
        Assert.assertEquals(VM_ID, (long)Long.valueOf(consumers.get(0).getUuid()));
        Assert.assertEquals(VM_DISPLAY_NAME, consumers.get(0).getDisplayName());
        Assert.assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), consumers.get(0).getClassName());

        // check tags
        Assert.assertEquals(0, result.getTags().size());

        // check that history stats service has been called correctly
        verify(historyService).getEntityStats(GetEntityStatsRequest.newBuilder()
            .setScope(
                EntityStatsScope.newBuilder()
                    .setEntityList(EntityList.newBuilder().addEntities(PM_ID)).build())
            .setFilter(
                StatsFilter.newBuilder()
                    .addCommodityRequests(
                        CommodityRequest.newBuilder()
                            .setCommodityName(EntitiesService.PRICE_INDEX_COMMODITY).build())
                    .build())
            .build());
    }

    /**
     * Searching for a non-existent entity should cause an {@link StatusRuntimeException}.
     *
     * @throws Exception expected: for entity not found.
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetEntityByUuidNonExistent() throws Exception {
        // add target and probe
        when(topologyProcessor.getTarget(Matchers.eq(TARGET_ID))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(PROBE_ID))).thenReturn(probeInfo);

        // call service and fail
        service.getEntityByUuid(Long.toString(NON_EXISTENT_ID), false);
    }

    /**
     * When topology processor fails to return information about the target
     * the whole process should fail.
     *
     * @throws Exception expected: target not found.
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetEntityByUuidMissingTarget() throws Exception {
        // error while fetching the target
        when(topologyProcessor.getTarget(Matchers.eq(TARGET_ID)))
            .thenThrow(new TopologyProcessorException("boom"));

        // call service and fail
        service.getEntityByUuid(Long.toString(ST_ID), false);
    }

    /**
     * When topology processor fails to return information about the probe
     * that discovers the target, the whole process should fail.
     *
     * @throws Exception expected: probe not found.
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetEntityByUuidMissingProbe() throws Exception {
        // error while fetching the target
        when(topologyProcessor.getTarget(Matchers.eq(TARGET_ID))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(PROBE_ID)))
            .thenThrow(new TopologyProcessorException("boom"));

        // call service and fail
        service.getEntityByUuid(Long.toString(ST_ID), false);
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
        when(topologyProcessor.getTarget(Matchers.eq(TARGET_ID))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(PROBE_ID))).thenReturn(probeInfo);

        // pretend that traversal queries will not work
        doAnswer(invocation -> {
            final SearchTopologyEntityDTOsRequest req =
                invocation.getArgumentAt(0, SearchTopologyEntityDTOsRequest.class);
            if (req.getSearchParametersCount() > 0) {
                return Optional.of(Status.INTERNAL.withDescription("traversal query failed")
                        .asException());
            } else {
                return Optional.empty();
            }
        }).when(searchService).searchTopologyEntityDTOsError(any());

        when(repositoryService.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder().addEntityOids(VM_ID).build()))
            .thenReturn(Collections.singletonList(EntityBatch.newBuilder().addEntities(VM).build()));

        // call service
        final ServiceEntityApiDTO result = service.getEntityByUuid(Long.toString(VM_ID), false);

        // check basic information
        Assert.assertEquals(Long.toString(VM_ID), result.getUuid());
        Assert.assertEquals(VM_DISPLAY_NAME, result.getDisplayName());
        Assert.assertEquals(
                EntityType.VIRTUAL_MACHINE_VALUE, UIEntityType.fromString(result.getClassName()).typeNumber());
        Assert.assertEquals(VM_STATE, UIEntityState.fromString(result.getState()).toEntityState());

        // check target information
        final TargetApiDTO resultTargetInfo = result.getDiscoveredBy();
        Assert.assertEquals(TARGET_ID, (long)Long.valueOf(resultTargetInfo.getUuid()));
        Assert.assertEquals(TARGET_DISPLAY_NAME, resultTargetInfo.getDisplayName());
        Assert.assertEquals(PROBE_TYPE, resultTargetInfo.getType());

        // there should no provider or consumer information; not even empty lists
        Assert.assertNull(result.getConsumers());
        Assert.assertNull(result.getProviders());

        // check tags
        Assert.assertEquals(1, result.getTags().size());
        Assert.assertArrayEquals(TAG_VALUES.toArray(), result.getTags().get(TAG_KEY).toArray());
    }

    /**
     * Get tags by entity id should work as expected, if tags exist.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetTags() throws Exception {
        // add target and probe
        when(topologyProcessor.getTarget(Matchers.eq(TARGET_ID))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(PROBE_ID))).thenReturn(probeInfo);

        doReturn(SearchTopologyEntityDTOsResponse.newBuilder()
            .addTopologyEntityDtos(VM)
            .build()).when(searchService).searchTopologyEntityDTOs(SearchTopologyEntityDTOsRequest.newBuilder()
                .addEntityOid(VM_ID)
                .build());

        // call service
        final List<TagApiDTO> result = service.getTagsByEntityUuid(Long.toString(VM_ID));

        // check tags
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TAG_KEY, result.get(0).getKey());
        Assert.assertArrayEquals(TAG_VALUES.toArray(), result.get(0).getValues().toArray());
    }

    /**
     * Get tags by entity id should work as expected, if tags don't exist.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetEmptyTags() throws Exception {
        // add target and probe
        when(topologyProcessor.getTarget(Matchers.eq(TARGET_ID))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(Matchers.eq(PROBE_ID))).thenReturn(probeInfo);

        doReturn(SearchTopologyEntityDTOsResponse.newBuilder()
            .addTopologyEntityDtos(PM)
            .build()).when(searchService).searchTopologyEntityDTOs(SearchTopologyEntityDTOsRequest.newBuilder()
            .addEntityOid(PM_ID)
            .build());

        // call service
        final List<TagApiDTO> result = service.getTagsByEntityUuid(Long.toString(PM_ID));

        // check tags
        Assert.assertEquals(0, result.size());
    }

    /**
     * An action returned by {@link EntitiesService#getActionByEntityUuid(String, String)}
     * will be obtained through the action orchestrator service and the action mapper.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetActionByEntityUuid() throws Exception {
        // fake an action and its translation by the ActionSpecMapper
        final ActionSpec dummyActionSpec = ActionSpec.getDefaultInstance();
        final ActionOrchestratorAction dummyActionOrchestratorResponse =
            ActionOrchestratorAction.newBuilder().setActionSpec(dummyActionSpec).build();
        final ActionApiDTO actionApiDTO = mock(ActionApiDTO.class);
        final ServiceEntityApiDTO entity = new ServiceEntityApiDTO();
        entity.setUuid(Long.toString(VM_ID));
        when(actionApiDTO.getCurrentEntity()).thenReturn(entity);
        when(actionsService.getAction(any())).thenReturn(dummyActionOrchestratorResponse);
        when(actionSpecMapper.mapActionSpecToActionApiDTO(
                Matchers.eq(dummyActionSpec), Matchers.eq(CONTEXT_ID)))
            .thenReturn(actionApiDTO);

        // call the service
        final long dummy = 0L;
        final ActionApiDTO result =
            service.getActionByEntityUuid(Long.toString(VM_ID), Long.toString(dummy));

        // check that the result is the faked translation
        Assert.assertEquals(actionApiDTO, result);
    }

    /**
     * An action returned by {@link EntitiesService#getActionsByEntityUuid}
     * will be obtained through the action orchestrator service and the action mapper.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetActionsByEntityUuid() throws Exception {
        // fake an action and its translation by the ActionSpecMapper
        final ActionSpec dummyActionSpec = ActionSpec.getDefaultInstance();
        final FilteredActionResponse dummyActionOrchestratorResponse =
            FilteredActionResponse.newBuilder()
                .addActions(ActionOrchestratorAction.newBuilder().setActionSpec(dummyActionSpec).build())
                .build();
        when(actionsService.getAllActions(any())).thenReturn(dummyActionOrchestratorResponse);
        final ActionApiDTO actionApiDTO = mock(ActionApiDTO.class);
        when(actionSpecMapper.mapActionSpecsToActionApiDTOs(any(), anyLong()))
            .thenReturn(Collections.singletonList(actionApiDTO));
        final ActionApiInputDTO trivialQuery = new ActionApiInputDTO();
        when(actionSpecMapper.createActionFilter(Matchers.eq(trivialQuery), any()))
            .thenReturn(ActionQueryFilter.getDefaultInstance());

        // call the service
        final ActionPaginationRequest paginationRequest =
            new ActionPaginationRequest(null, 1, false, null);
        final ActionApiDTO result =
            service
                .getActionsByEntityUuid(Long.toString(VM_ID), trivialQuery, paginationRequest)
                .getRawResults()
                .get(0);

        // check that the result is the faked translation
        Assert.assertEquals(actionApiDTO, result);
    }
}
