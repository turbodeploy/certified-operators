package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.aspect.CloudAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualMachineAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.SearchPlanTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Test the construction of {@link ActionApiDTO} with compound moves.
 */
public class CompoundMoveTest {

    private static final long REAL_TIME_TOPOLOGY_CONTEXT_ID = 777777L;

    private ActionSpecMapper mapper;

    private PolicyDTOMoles.PolicyServiceMole policyMole;

    private RepositoryApi repositoryApi;

    private GrpcTestServer grpcServer;

    private PolicyServiceGrpc.PolicyServiceBlockingStub policyService;

    private ActionSpecMappingContextFactory actionSpecMappingContextFactory;

    private static final int VM = EntityType.VIRTUAL_MACHINE_VALUE;
    private static final int PM = EntityType.PHYSICAL_MACHINE_VALUE;
    private static final int ST = EntityType.STORAGE_VALUE;

    private static final long TARGET_ID = 100L;
    private static final String TARGET_DISPLAY_NAME = "target display name";
    private static final String PROBE_TYPE = "probe type";

    private SearchServiceMole searchMole;

    private static final long TARGET_ENTITY_ID = 10;
    private static final String TARGET_NAME = "vm-1";
    private static final long ST1_ID = 20;
    private static final String ST1_NAME = "storage-1";
    private static final long ST2_ID = 21;
    private static final String ST2_NAME = "storage-2";
    private static final long PM1_ID = 30;
    private static final String PM1_NAME = "host-1";
    private static final long PM2_ID = 31;
    private static final String PM2_NAME = "host-2";
    private static final long VOL1_ID = 40;
    private static final String VOL1_NAME = "vol-1";

    private TopologyProcessor topologyProcessor = Mockito.mock(TopologyProcessor.class);

    private final ServiceEntityMapper serviceEntityMapper = new ServiceEntityMapper(topologyProcessor);

    @Before
    public void setup() throws Exception {
        policyMole = Mockito.spy(PolicyDTOMoles.PolicyServiceMole.class);
        final List<PolicyResponse> policyResponses = ImmutableList.of(
            PolicyResponse.newBuilder().setPolicy(Policy.newBuilder()
                .setId(1)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setName("policy")))
                .build());
        Mockito.when(policyMole.getAllPolicies(Mockito.any())).thenReturn(policyResponses);
        grpcServer = GrpcTestServer.newServer(policyMole);
        grpcServer.start();
        policyService = PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        repositoryApi = Mockito.mock(RepositoryApi.class);
        searchMole = Mockito.spy(new SearchServiceMole());
        GrpcTestServer searchGrpcServer = GrpcTestServer.newServer(searchMole);
        searchGrpcServer.start();
        SearchServiceBlockingStub searchServiceBlockingStub = SearchServiceGrpc.newBlockingStub(
            searchGrpcServer.getChannel());

        mockTopologyProcessorOutputs();

        actionSpecMappingContextFactory = new ActionSpecMappingContextFactory(policyService,
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().build()),
            searchServiceBlockingStub, Mockito.mock(CloudAspectMapper.class),
            Mockito.mock(VirtualMachineAspectMapper.class),
            Mockito.mock(VirtualVolumeAspectMapper.class), REAL_TIME_TOPOLOGY_CONTEXT_ID,
            Mockito.mock(CostClientConfig.class), null,
            null, serviceEntityMapper);

        mapper = new ActionSpecMapper(actionSpecMappingContextFactory, Mockito.mock(ServiceEntityMapper.class), REAL_TIME_TOPOLOGY_CONTEXT_ID,
                 Mockito.mock(CostClientConfig.class), Mockito.mock(CommunicationConfig.class),
                 Mockito.mock(MapperConfig.class));
        IdentityGenerator.initPrefix(0);
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(TARGET_NAME, TARGET_ENTITY_ID, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO(PM1_NAME, PM1_ID, EntityType.PHYSICAL_MACHINE_VALUE),
                    topologyEntityDTO(PM2_NAME, PM2_ID, EntityType.PHYSICAL_MACHINE_VALUE),
                    topologyEntityDTO(ST1_NAME, ST1_ID, EntityType.STORAGE_VALUE),
                    topologyEntityDTO(ST2_NAME, ST2_ID, EntityType.STORAGE_VALUE),
                    topologyEntityDTO(VOL1_NAME, VOL1_ID, EntityType.VIRTUAL_VOLUME_VALUE)))
                .build());
    }

    private TopologyEntityDTO topologyEntityDTO(@Nonnull final String displayName, long oid,
                                                int entityType) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName(displayName)
                .setEntityType(entityType)
                .setOrigin(
                    Origin.newBuilder()
                        .setDiscoveryOrigin(DiscoveryOrigin.newBuilder().addDiscoveringTargetIds(TARGET_ID)))
                .build();
    }

    /**
     * Test a compound move with one provider being changed.
     *
     * @throws UnknownObjectException not supposed to happen
     * @throws UnsupportedActionException  not supposed to happen
     */
    @Test
    public void testSimpleAction()
                    throws UnknownObjectException, UnsupportedActionException, ExecutionException,
                    InterruptedException {
        ActionDTO.Action moveStorage = makeAction(TARGET_ENTITY_ID, VM, ST1_ID, ST, ST2_ID, ST, null);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveStorage), 77L);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(1, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(ST1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(ST2_ID), apiDto.getNewValue());
        checkTargetsInActionApiDTO(apiDto);
    }

    /**
     * Test a simple move with one provider being changed. This change is for a particular
     * resource of the target.
     *
     * @throws UnknownObjectException not supposed to happen
     * @throws UnsupportedActionException  not supposed to happen
     */
    @Test
    public void testSimpleActionWithResource()
            throws UnknownObjectException, UnsupportedActionException, ExecutionException,
            InterruptedException {
        ActionDTO.Action moveVolume = makeAction(TARGET_ENTITY_ID, VM, ST1_ID, ST, ST2_ID, ST, VOL1_ID);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveVolume), 77L);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(1, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(ST1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(ST2_ID), apiDto.getNewValue());
        checkTargetsInActionApiDTO(apiDto);
    }

    /**
     * Test a compound move with two providers being changed, first
     * is Storage, second is PM.
     *
     * @throws UnknownObjectException not supposed to happen
     * @throws UnsupportedActionException  not supposed to happen
     */
    @Test
    public void testCompoundAction1()
                    throws UnknownObjectException, UnsupportedActionException, ExecutionException,
                    InterruptedException {
        ActionDTO.Action moveBoth1 = makeAction(TARGET_ENTITY_ID, VM, ST1_ID, ST, ST2_ID, ST, PM1_ID, PM, PM2_ID, PM);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveBoth1), 77L);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(2, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(PM1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(PM2_ID), apiDto.getNewValue());
        checkTargetsInActionApiDTO(apiDto);
    }

    /**
     * Test a compound move with two providers being changed, first
     * is PM, second is Storage.
     *
     * @throws UnknownObjectException not supposed to happen
     * @throws UnsupportedActionException  not supposed to happen
     */
    @Test
    public void testCompoundAction2()
                    throws UnknownObjectException, UnsupportedActionException, ExecutionException,
                    InterruptedException {
        ActionDTO.Action moveBoth2 = makeAction(TARGET_ENTITY_ID, VM,
            PM1_ID, PM, PM2_ID, PM, ST1_ID, ST, ST2_ID, ST); // different order
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveBoth2), 77L);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(2, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(PM1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(PM2_ID), apiDto.getNewValue());
        checkTargetsInActionApiDTO(apiDto);
    }

    private ActionSpec buildActionSpec(ActionDTO.Action action) {
        return ActionSpec.newBuilder()
            .setRecommendationTime(System.currentTimeMillis())
            .setRecommendation(action)
            .setActionState(ActionState.READY)
            .setActionMode(ActionMode.MANUAL)
            .setIsExecutable(true)
            .setExplanation("default explanation")
            .build();
    }

    private static ActionDTO.Action makeAction(long t, int tType,
                                               long s1, int s1Type,
                                               long d1, int d1Type,
                                               Long r1) {
        return genericActionStuff()
                        .setInfo(ActionInfo.newBuilder()
                            .setMove(Move.newBuilder()
                                .setTarget(ApiUtilsTest.createActionEntity(t, tType))
                                .addChanges(makeChange(s1, s1Type, d1, d1Type, r1))
                                .build())
                            .build())
                        .build();
    }

    private static ActionDTO.Action makeAction(long t, int tType,
                                               long s1, int s1Type,
                                               long d1, int d1Type,
                                               long s2, int s2Type,
                                               long d2, int d2Type) {
        return genericActionStuff()
                        .setInfo(ActionInfo.newBuilder()
                            .setMove(Move.newBuilder()
                                .setTarget(ApiUtilsTest.createActionEntity(t, tType))
                                .addChanges(makeChange(s1, s1Type, d1, d1Type, null))
                                .addChanges(makeChange(s2, s2Type, d2, d2Type, null))
                                .build())
                            .build())
                        .build();
    }

    private static ActionDTO.Action.Builder genericActionStuff() {
        return ActionDTO.Action.newBuilder()
                        .setId(IdentityGenerator.next())
                        .setImportance(0)
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setExplanation(Explanation.newBuilder().build());
    }

    private static ChangeProvider makeChange(long source, int sourceType,
                                             long destination, int destType,
                                             Long resource) {
        Builder changeProviderBuilder = ChangeProvider.newBuilder()
                        .setSource(ApiUtilsTest.createActionEntity(source, sourceType))
                        .setDestination(ApiUtilsTest.createActionEntity(destination, destType));
        if (resource != null) {
            changeProviderBuilder.setResource(ApiUtilsTest.createActionEntity(resource));
        }
        return changeProviderBuilder.build();
    }

    private void mockTopologyProcessorOutputs() throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        final ProbeInfo probeInfo = Mockito.mock(ProbeInfo.class);
        final AccountValue accountValue = Mockito.mock(AccountValue.class);
        Mockito.when(targetInfo.getId()).thenReturn(TARGET_ID);
        final long probeId = 11L;
        Mockito.when(targetInfo.getProbeId()).thenReturn(probeId);
        Mockito.when(targetInfo.getAccountData()).thenReturn(Collections.singleton(accountValue));
        Mockito.when(accountValue.getName()).thenReturn(TargetInfo.TARGET_ADDRESS);
        Mockito.when(accountValue.getStringValue()).thenReturn(TARGET_DISPLAY_NAME);
        Mockito.when(probeInfo.getId()).thenReturn(probeId);
        Mockito.when(probeInfo.getType()).thenReturn(PROBE_TYPE);
        Mockito.when(topologyProcessor.getProbe(Mockito.eq(probeId))).thenReturn(probeInfo);
        Mockito.when(topologyProcessor.getTarget(Mockito.eq(TARGET_ID))).thenReturn(targetInfo);
    }

    private void checkTargetsInActionApiDTO(ActionApiDTO actionApiDTO) {
        Assert.assertNotNull(actionApiDTO);
        Stream.of(actionApiDTO.getTarget(), actionApiDTO.getNewEntity(), actionApiDTO.getCurrentEntity())
                .filter(Objects::nonNull)
                .forEach(e -> checkTarget(e.getDiscoveredBy()));
    }

    private void checkTarget(TargetApiDTO targetApiDTO) {
        Assert.assertNotNull(targetApiDTO);
        Assert.assertEquals(TARGET_ID, (long)Long.valueOf(targetApiDTO.getUuid()));
        Assert.assertEquals(TARGET_DISPLAY_NAME, targetApiDTO.getDisplayName());
        Assert.assertEquals(PROBE_TYPE, targetApiDTO.getType());
    }
}
