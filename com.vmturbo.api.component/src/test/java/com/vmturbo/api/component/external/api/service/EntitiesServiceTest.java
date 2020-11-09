package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse.ActionChunk;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.repository.EntityConstraintsServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reporting.api.protobuf.ReportingMoles.ReportingServiceMole;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Tests for {@link EntitiesService}.
 */
public class EntitiesServiceTest {
    // service under test
    private EntitiesService service;

    // mocked services and mappers
    private final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);
    private final ActionSpecMapper actionSpecMapper = mock(ActionSpecMapper.class);
    private final UuidMapper uuidMapper = mock(UuidMapper.class);
    private final GroupExpander groupExpander = mock(GroupExpander.class);

    // data objects
    private TargetInfo targetInfo;
    private ProbeInfo probeInfo;

    private final SeverityPopulator severityPopulator = mock(SeverityPopulator.class);
    private final PriceIndexPopulator priceIndexPopulator = mock(PriceIndexPopulator.class);
    private final ActionsServiceMole actionsService = spy(new ActionsServiceMole());
    private final GroupServiceMole groupService = spy(new GroupServiceMole());
    private final StatsHistoryServiceMole historyService = spy(new StatsHistoryServiceMole());
    private final ReportingServiceMole reportingService = spy(new ReportingServiceMole());
    private final RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private final SupplyChainFetcherFactory supplyChainFetcherFactory =
            mock(SupplyChainFetcherFactory.class);
    private final PolicyServiceMole policyService = spy(new PolicyServiceMole());

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private EntitySettingQueryExecutor entitySettingQueryExecutor = mock(EntitySettingQueryExecutor.class);

    private final ServiceProviderExpander serviceProviderExpander = mock(ServiceProviderExpander.class);

    private ServiceEntityMapper serviceEntityMapper = mock(ServiceEntityMapper.class);

    private SettingsManagerMapping settingsManagerMapping = mock(SettingsManagerMapping.class);

    /**
     * Exception checker.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // gRPC servers
    @Rule
    public final GrpcTestServer grpcServer =
        GrpcTestServer.newServer(actionsService, groupService, historyService, reportingService,
            repositoryService, policyService);
    private final ThinTargetCache thinTargetCache = mock(ThinTargetCache.class);

    // a sample topology ST -> PM -> VM
    private static final long CONTEXT_ID = 777777L;
    private static final String UI_REAL_TIME_MARKET = "Market";
    private static final String MARKET_DISPLAY_NAME = "ST";
    private static final EntityState MARKET_STATE = EntityState.POWERED_ON;
    private static final long TARGET_ID = 7L;
    private static final String TARGET_DISPLAY_NAME = "target";
    private static final long PROBE_ID = 70L;
    private static final String PROBE_TYPE = "probe";
    private static final long VM_ID = 1L;
    private static final long GROUP_ID = 2L;
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
    private static final String LOCAL_NAME = "qqq";
    private static final PerTargetEntityInformation PER_TARGET_INFO = PerTargetEntityInformation
                    .newBuilder().setVendorId(LOCAL_NAME).build();

    private static final ApiPartialEntity VM =
        ApiPartialEntity.newBuilder()
            .setOid(VM_ID)
            .setDisplayName(VM_DISPLAY_NAME)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(VM_STATE)
            .putDiscoveredTargetData(TARGET_ID, PER_TARGET_INFO)
            .setTags(
                    Tags.newBuilder()
                            .putTags(
                                    TAG_KEY,
                                    TagValuesDTO.newBuilder().addAllValues(TAG_VALUES).build())
                            .build())
            .build();
    private static final ApiPartialEntity PM =
        ApiPartialEntity.newBuilder()
            .setOid(PM_ID)
            .setDisplayName(PM_DISPLAY_NAME)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setEntityState(PM_STATE)
            .putDiscoveredTargetData(TARGET_ID, PER_TARGET_INFO)
            .build();

    private static final ApiPartialEntity ST =
        ApiPartialEntity.newBuilder()
            .setOid(ST_ID)
            .setDisplayName(ST_DISPLAY_NAME)
            .setEntityType(EntityType.STORAGE_VALUE)
            .setEntityState(ST_STATE)
            .putDiscoveredTargetData(TARGET_ID, PER_TARGET_INFO)
            .build();

    /**
     * Set up a mock topology processor server and a {@link ProbesService} client and connects them.
     */
    @Before
    public void setUp() {
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
        final ActionsServiceBlockingStub actionOrchestratorRpcService =
            ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final PaginationMapper paginationMapper = new PaginationMapper();

        final ActionSearchUtil actionSearchUtil =
            new ActionSearchUtil(
                actionOrchestratorRpcService, actionSpecMapper,
                paginationMapper, supplyChainFetcherFactory, groupExpander,
                    serviceProviderExpander,
                    CONTEXT_ID);

        when(groupExpander.expandOids(any()))
            .thenAnswer(invocation -> invocation.getArgumentAt(0, Set.class));

        // Create service
        service =
            new EntitiesService(
                actionOrchestratorRpcService,
                actionSpecMapper,
                CONTEXT_ID,
                supplyChainFetcherFactory,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                mock(EntityAspectMapper.class),
                severityPopulator,
                priceIndexPopulator,
                mock(StatsService.class),
                mock(ActionStatsQueryExecutor.class),
                uuidMapper,
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                mock(SettingsMapper.class),
                actionSearchUtil,
                repositoryApi, entitySettingQueryExecutor,
                EntityConstraintsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                thinTargetCache,
                paginationMapper,
                serviceEntityMapper,
                settingsManagerMapping);
    }

    /**
     * Tests the normal behavior of the {@link EntitiesService#getEntityByUuid(String, boolean)}
     * method, without aspects.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetEntityByUuid() throws Exception {
        ServiceEntityApiDTO pm = new ServiceEntityApiDTO();
        pm.setUuid(Long.toString(PM_ID));
        pm.setDisplayName(PM_DISPLAY_NAME);

        MinimalEntity vm = MinimalEntity.newBuilder()
            .setOid(VM.getOid())
            .setEntityType(VM.getEntityType())
            .setDisplayName(VM.getDisplayName())
            .build();

        MinimalEntity st = MinimalEntity.newBuilder()
            .setOid(ST.getOid())
            .setEntityType(ST.getEntityType())
            .setDisplayName(ST.getDisplayName())
            .build();

        SingleEntityRequest pmReq = ApiTestUtils.mockSingleEntityRequest(pm);
        when(repositoryApi.entityRequest(PM_ID)).thenReturn(pmReq);

        SearchRequest s1Req = ApiTestUtils.mockSearchMinReq(Lists.newArrayList(vm));
        when(repositoryApi.newSearchRequest(SearchProtoUtil.neighbors(PM_ID, TraversalDirection.PRODUCES)))
            .thenReturn(s1Req);

        SearchRequest s2Req = ApiTestUtils.mockSearchMinReq(Lists.newArrayList(st));
        when(repositoryApi.newSearchRequest(SearchProtoUtil.neighbors(PM_ID, TraversalDirection.CONSUMES)))
            .thenReturn(s2Req);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(PM_ID);
        when(apiId.isEntity()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(PM_ID))).thenReturn(apiId);

        // call service
        final ServiceEntityApiDTO result = service.getEntityByUuid(Long.toString(PM_ID), false);

        // check basic information
        Assert.assertEquals(Long.toString(PM_ID), result.getUuid());
        Assert.assertEquals(PM_DISPLAY_NAME, result.getDisplayName());

        // check providers and consumers
        final List<BaseApiDTO> providers = result.getProviders();
        Assert.assertEquals(1, providers.size());
        Assert.assertEquals(ST_ID, (long)Long.valueOf(providers.get(0).getUuid()));
        Assert.assertEquals(ST_DISPLAY_NAME, providers.get(0).getDisplayName());
        Assert.assertEquals(ApiEntityType.STORAGE.apiStr(), providers.get(0).getClassName());
        final List<BaseApiDTO> consumers = result.getConsumers();
        Assert.assertEquals(1, consumers.size());
        Assert.assertEquals(VM_ID, (long)Long.valueOf(consumers.get(0).getUuid()));
        Assert.assertEquals(VM_DISPLAY_NAME, consumers.get(0).getDisplayName());
        Assert.assertEquals(ApiEntityType.VIRTUAL_MACHINE.apiStr(), consumers.get(0).getClassName());

        // check that priceIndexPopulator has been called correctly
        verify(priceIndexPopulator).populateRealTimeEntities(eq(Collections.singletonList(result)));
    }

    /**
     * Tests the illegal entity id 'Market'.
     *
     * @throws Exception in case of illegal entity id.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetEntityByStringUuid() throws Exception {
        ApiId apiId = mock(ApiId.class);
        when(apiId.isEntity()).thenReturn(false);
        when(uuidMapper.fromUuid(UI_REAL_TIME_MARKET)).thenReturn(apiId);

        // call service and fail
        service.getEntityByUuid(UI_REAL_TIME_MARKET, false);
    }

    /**
     * Searching for a non-existent entity should cause an {@link StatusRuntimeException}.
     *
     * @throws Exception expected: for entity not found.
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetEntityByUuidNonExistent() throws Exception {
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityEmptyRequest();
        when(repositoryApi.entityRequest(NON_EXISTENT_ID)).thenReturn(req);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(NON_EXISTENT_ID);
        when(apiId.isEntity()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(NON_EXISTENT_ID))).thenReturn(apiId);

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
        SingleEntityRequest req = ApiTestUtils.mockSingleEntityEmptyRequest();
        when(repositoryApi.entityRequest(ST_ID)).thenReturn(req);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(ST_ID);
        when(apiId.isEntity()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(ST_ID))).thenReturn(apiId);

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
        final ServiceEntityApiDTO vm = new ServiceEntityApiDTO();
        vm.setUuid(Long.toString(VM.getOid()));
        vm.setDisplayName(VM.getDisplayName());

        final SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(vm);
        when(repositoryApi.entityRequest(VM_ID)).thenReturn(req);

        SearchRequest relatedReq = ApiTestUtils.mockSearchMinReq(Collections.emptyList());
        when(relatedReq.getMinimalEntities())
            .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));
        when(repositoryApi.newSearchRequest(any(SearchParameters.class))).thenReturn(relatedReq);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.isEntity()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        // call service
        final ServiceEntityApiDTO result = service.getEntityByUuid(Long.toString(VM_ID), false);

        // check basic information
        Assert.assertEquals(Long.toString(VM_ID), result.getUuid());
        Assert.assertEquals(VM_DISPLAY_NAME, result.getDisplayName());

        // there should no provider or consumer information; not even empty lists
        Assert.assertNull(result.getConsumers());
        Assert.assertNull(result.getProviders());
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

        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(VM);
        when(repositoryApi.entityRequest(VM_ID)).thenReturn(req);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.isEntity()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        // call service
        final List<TagApiDTO> result = service.getTagsByEntityUuid(Long.toString(VM_ID));

        // check tags
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TAG_KEY, result.get(0).getKey());
        Assert.assertArrayEquals(TAG_VALUES.toArray(), result.get(0).getValues().toArray());
    }

    /**
     * Test get tags for groups.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetTagsForGroup() throws Exception {
        final GetTagsResponse tagsForGroupsResponse = GetTagsResponse.newBuilder()
                .putTags(GROUP_ID, Tags.newBuilder()
                        .putTags(TAG_KEY,
                                TagValuesDTO.newBuilder().addAllValues(TAG_VALUES).build())
                        .build())
                .build();
        when(groupService.getTags(
                GetTagsRequest.newBuilder().addGroupId(GROUP_ID).build())).thenReturn(
                tagsForGroupsResponse);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(GROUP_ID);
        when(apiId.isEntity()).thenReturn(false);
        when(apiId.isGroup()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(GROUP_ID))).thenReturn(apiId);

        final List<TagApiDTO> tags = service.getTagsByEntityUuid(String.valueOf(GROUP_ID));
        Assert.assertEquals(1, tags.size());
        final TagApiDTO tagInfo = tags.iterator().next();
        Assert.assertEquals(TAG_KEY, tagInfo.getKey());
        Assert.assertEquals(TAG_VALUES, tagInfo.getValues());
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

        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(PM);
        when(repositoryApi.entityRequest(PM_ID)).thenReturn(req);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(PM_ID);
        when(apiId.isEntity()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(PM_ID))).thenReturn(apiId);

        // call service
        final List<TagApiDTO> result = service.getTagsByEntityUuid(Long.toString(PM_ID));

        // check tags
        Assert.assertEquals(0, result.size());
    }

    /**
     * An action returned by {@link EntitiesService#getActionByEntityUuid(String, String,
     * com.vmturbo.api.enums.ActionDetailLevel)}.
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
                Matchers.eq(dummyActionSpec), Matchers.eq(CONTEXT_ID), Matchers.eq(ActionDetailLevel.STANDARD)))
            .thenReturn(actionApiDTO);

        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(VM);
        when(repositoryApi.entityRequest(VM_ID)).thenReturn(req);

        final long dummy = 0L;

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(dummy);
        when(apiId.isEntity()).thenReturn(true);
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);
        when(uuidMapper.fromUuid(Long.toString(dummy))).thenReturn(apiId);

        // call the service
        final ActionApiDTO result =
                service.getActionByEntityUuid(Long.toString(VM_ID), Long.toString(dummy), ActionDetailLevel.STANDARD);

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
                    .setActionChunk(ActionChunk.newBuilder()
                            .addActions(ActionOrchestratorAction.newBuilder()
                                    .setActionSpec(dummyActionSpec)))
                    .build();

        final MinimalEntity minimalEntityVM = MinimalEntity.newBuilder()
                .setOid(VM_ID)
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setDisplayName("VM")
                .build();

        when(actionsService.getAllActions(any())).thenReturn(Collections.singletonList(dummyActionOrchestratorResponse));
        final ActionApiDTO actionApiDTO = mock(ActionApiDTO.class);
        when(actionSpecMapper.mapActionSpecsToActionApiDTOs(any(), anyLong(), any()))
            .thenReturn(Collections.singletonList(actionApiDTO));
        final ActionApiInputDTO trivialQuery = new ActionApiInputDTO();
        when(actionSpecMapper.createActionFilter(Matchers.eq(trivialQuery), any(), any()))
            .thenReturn(ActionQueryFilter.getDefaultInstance());

        SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(VM);
        when(repositoryApi.entityRequest(VM_ID)).thenReturn(req);

        ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(VM_ID);
        when(apiId.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE)));
        when(uuidMapper.fromUuid(Long.toString(VM_ID))).thenReturn(apiId);

        RepositoryApi.MultiEntityRequest minimalEntityVMRequest = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(minimalEntityVM));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(Long.valueOf(VM_ID)))).thenReturn(minimalEntityVMRequest);

        Set<Long> scope = Collections.singleton(VM_ID);
        when(serviceProviderExpander.expand(scope)).thenReturn(scope);
        when(supplyChainFetcherFactory.expandAggregatedEntities(scope)).thenReturn(scope);

        when(groupExpander.expandOids(Collections.singleton(apiId))).thenReturn(Collections.singleton(VM_ID));

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

        long regionId = 2L;
        ApiId regionApiId = mock(ApiId.class);
        when(regionApiId.oid()).thenReturn(regionId);
        when(regionApiId.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(ApiEntityType.REGION)));
        when(uuidMapper.fromUuid(Long.toString(regionId))).thenReturn(regionApiId);


        when(groupExpander.expandOids(Collections.singleton(regionApiId))).thenReturn(Collections.singleton(regionId));

        final MinimalEntity minimalEntityRegion = MinimalEntity.newBuilder()
                .setOid(regionId)
                .setEntityType(ApiEntityType.REGION.typeNumber())
                .setDisplayName("Region1")
                .build();

        RepositoryApi.MultiEntityRequest minimalEntityRegionRequest = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(minimalEntityRegion));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(regionId))).thenReturn(minimalEntityRegionRequest);

        scope = Collections.singleton(regionId);
        when(serviceProviderExpander.expand(scope)).thenReturn(scope);
        when(supplyChainFetcherFactory.expandAggregatedEntities(scope)).thenReturn(scope);

        final ActionApiDTO regionResult =
            service
                .getActionsByEntityUuid(Long.toString(regionId), trivialQuery, paginationRequest)
                .getRawResults()
                .get(0);

        Assert.assertEquals(actionApiDTO, regionResult);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior to incorrect input (incorrect
     * entity uuid).
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityWithIncorrectEntityUuid() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final String settingUuid = "usedIncrement_VCPU";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);

        // incorrect entity uuid provided
        when(apiId.isEntity()).thenReturn(false);
        expectedException.expect(IllegalArgumentException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior to incorrect input (incorrect
     * setting uuid).
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityWithIncorrectSettingUuid() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        // invalid setting uuid
        final String settingUuid = "aaaa";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        when(apiId.isEntity()).thenReturn(true);

        expectedException.expect(IllegalArgumentException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior to incorrect input (incorrect
     * settings manager uuid).
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityWithIncorrectSettingsManagerUuid() throws Exception {
        final String entityUuid = "1234";
        // incorrect settings manager uuid
        final String settingsManagerUuid = "aaaa";
        final String settingUuid = "usedIncrement_VCPU";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        when(apiId.isEntity()).thenReturn(true);
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.empty());
        expectedException.expect(IllegalArgumentException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior to incorrect input (setting not
     * belonging to the settings manager provided).
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityWithIncorrectSettingAndSettingsManagerCombination()
            throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final String settingUuid = "smtpServer";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        when(apiId.isEntity()).thenReturn(true);
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        // settings manager provided does not contain the setting provided
        when(settingsManagerInfo.getSettings())
                .thenReturn(Collections.singleton("usedIncrement_VCPU"));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));
        expectedException.expect(IllegalArgumentException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior to incorrect input (setting not
     * valid for the type of the entity provided).
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityWithIncorrectSettingForEntityTypeCombination()
            throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final String settingUuid = "usedIncrement_VCPU";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                // entity type does not corresponds with the entity uuid
                .setEntityType(EntityType.STORAGE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        // settings manager provided does not contain the setting provided
        when(settingsManagerInfo.getSettings())
                .thenReturn(Collections.singleton("usedIncrement_VCPU"));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));
        expectedException.expect(IllegalArgumentException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior when the result from
     * entitySettingQuery is empty.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityOnEmptyEntitySettingQueryResult() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final String settingUuid = "usedIncrement_VCPU";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings()).thenReturn(Collections.singleton(settingUuid));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));
        // empty result from entitySettingQuery
        when(entitySettingQueryExecutor.getEntitySettings(
                apiId, false, Collections.singletonList(settingUuid), null))
                .thenReturn(Collections.emptyList());
        expectedException.expect(OperationFailedException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior when the settings manager cannot
     * be found.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityWhenManagerIsNotFound() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final String settingUuid = "usedIncrement_VCPU";
        final SettingsManagerApiDTO resultManagerDto = new SettingsManagerApiDTO();
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings()).thenReturn(Collections.singleton(settingUuid));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));
        // managers in the response do not include marketsettingsmanager
        resultManagerDto.setUuid("automationmanager");
        when(entitySettingQueryExecutor.getEntitySettings(
                apiId, false, Collections.singletonList(settingUuid), null))
                .thenReturn(Collections.singletonList(resultManagerDto));
        expectedException.expect(OperationFailedException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior when the settingsmanager returned
     * from group component does not have settings.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityWhenManagerDoesNotHaveSettings() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final String settingUuid = "usedIncrement_VCPU";
        final SettingsManagerApiDTO resultManagerDto = new SettingsManagerApiDTO();
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings()).thenReturn(Collections.singleton(settingUuid));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));
        resultManagerDto.setUuid(settingsManagerUuid);
        // no settings added to resultManagerDto
        when(entitySettingQueryExecutor.getEntitySettings(
                apiId, false, Collections.singletonList(settingUuid), null))
                .thenReturn(Collections.singletonList(resultManagerDto));
        expectedException.expect(OperationFailedException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior when the settingsmanager returned
     * from group component does not have the setting specified.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntityWhenManagerDoesNotHaveTheSpecifiedSetting() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final String settingUuid = "usedIncrement_VCPU";
        final SettingsManagerApiDTO resultManagerDto = new SettingsManagerApiDTO();
        final SettingApiDTO<String> resultSettingDto = new SettingApiDTO<>();
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings()).thenReturn(Collections.singleton(settingUuid));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));
        resultManagerDto.setUuid(settingsManagerUuid);
        resultSettingDto.setUuid("cpuUtilization");
        resultManagerDto.setSettings(Collections.singletonList(resultSettingDto));
        when(entitySettingQueryExecutor.getEntitySettings(
                apiId, false, Collections.singletonList(settingUuid), null))
                .thenReturn(Collections.singletonList(resultManagerDto));
        expectedException.expect(OperationFailedException.class);
        service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingByEntity}'s behavior on correct input.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingByEntity() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final String settingUuid = "usedIncrement_VCPU";
        final SettingApiDTO<String> resultSettingDto = new SettingApiDTO<>();
        final SettingsManagerApiDTO resultManagerDto = new SettingsManagerApiDTO();
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings()).thenReturn(Collections.singleton(settingUuid));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));

        // correct behavior
        resultManagerDto.setUuid(settingsManagerUuid);
        resultSettingDto.setUuid(settingUuid);
        resultManagerDto.setSettings(Collections.singletonList(resultSettingDto));
        when(entitySettingQueryExecutor.getEntitySettings(
                apiId, false, Collections.singletonList(settingUuid), null))
                .thenReturn(Collections.singletonList(resultManagerDto));
        SettingApiDTO result =
                service.getSettingByEntity(entityUuid, settingsManagerUuid, settingUuid);
        Assert.assertEquals(settingUuid, result.getUuid());
    }

    /**
     * Tests {@link EntitiesService#getSettingsByEntityAndManager}'s behavior to incorrect input
     * (incorrect entity uuid).
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingsByEntityAndManagerWithIncorrectEntityUuid() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));

        // incorrect uuid provided
        when(apiId.isEntity()).thenReturn(false);
        expectedException.expect(IllegalArgumentException.class);
        service.getSettingsByEntityAndManager(entityUuid, settingsManagerUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingsByEntityAndManager}'s behavior to incorrect input
     * (incorrect settings manager uuid).
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingsByEntityAndManagerWithIncorrectSettingsManagerUuid()
            throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        // incorrect settings manager uuid -> no manager info object found
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.empty());

        expectedException.expect(IllegalArgumentException.class);
        service.getSettingsByEntityAndManager(entityUuid, settingsManagerUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingsByEntityAndManager}'s behavior to incorrect input
     * (invalid settings manager for entity type).
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingsByEntityAndManagerWithInvalidSettingsManagerForEntityType()
            throws Exception {
        final String entityUuid = "1234";
        // emailmanager is an invalid settings manager for VMs
        final String settingsManagerUuid = "emailmanager";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings()).thenReturn(Collections.singleton("smtpPort"));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));

        expectedException.expect(IllegalArgumentException.class);
        service.getSettingsByEntityAndManager(entityUuid, settingsManagerUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingsByEntityAndManager}'s behavior when the result from
     * entitySettingQuery is empty.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingsByEntityAndManagerOnEmptyEntitySettingQueryResult() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings())
                .thenReturn(Collections.singleton("usedIncrement_VCPU"));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));

        // empty result from entitySettingQuery
        when(entitySettingQueryExecutor.getEntitySettings(apiId, false, null, null))
                .thenReturn(Collections.emptyList());
        expectedException.expect(OperationFailedException.class);
        service.getSettingsByEntityAndManager(entityUuid, settingsManagerUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingsByEntityAndManager}'s behavior when settings manager
     * is not found.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingsByEntityAndManagerWhenManagerIsNotFound() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final SettingsManagerApiDTO resultManagerDto = new SettingsManagerApiDTO();
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings())
                .thenReturn(Collections.singleton("usedIncrement_VCPU"));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));
        // settings manager not found
        resultManagerDto.setUuid("automationmanager");
        when(entitySettingQueryExecutor.getEntitySettings(apiId, false, null, null))
                .thenReturn(Collections.singletonList(resultManagerDto));
        expectedException.expect(OperationFailedException.class);
        service.getSettingsByEntityAndManager(entityUuid, settingsManagerUuid);
    }

    /**
     * Tests {@link EntitiesService#getSettingsByEntityAndManager}'s behavior on correct input.
     *
     * @throws Exception on error
     */
    @Test
    public void testGetSettingsByEntityAndManager() throws Exception {
        final String entityUuid = "1234";
        final String settingsManagerUuid = "marketsettingsmanager";
        final SettingApiDTO<String> resultSettingDto = new SettingApiDTO<>();
        final SettingsManagerApiDTO resultManagerDto = new SettingsManagerApiDTO();
        ApiId apiId = mock(ApiId.class);
        when(apiId.isGroup()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        MinimalEntity minimalEntity = MinimalEntity.newBuilder()
                .setOid(Long.parseLong(entityUuid))
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        CachedEntityInfo cachedEntityInfo = new CachedEntityInfo(minimalEntity);
        when(apiId.getCachedEntityInfo()).thenReturn(Optional.of(cachedEntityInfo));
        when(uuidMapper.fromUuid(entityUuid)).thenReturn(apiId);
        final SettingsManagerInfo settingsManagerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerInfo.getSettings())
                .thenReturn(Collections.singleton("usedIncrement_VCPU"));
        when(settingsManagerMapping.getManagerInfo(settingsManagerUuid))
                .thenReturn(Optional.of(settingsManagerInfo));

        // correct behavior
        resultManagerDto.setUuid(settingsManagerUuid);
        resultManagerDto.setSettings(Collections.singletonList(resultSettingDto));
        when(entitySettingQueryExecutor.getEntitySettings(apiId, false, null, null))
                .thenReturn(Collections.singletonList(resultManagerDto));
        List<? extends SettingApiDTO> results =
                service.getSettingsByEntityAndManager(entityUuid, settingsManagerUuid);
        Assert.assertFalse(results.isEmpty());
    }
}
