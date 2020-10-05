package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.service.ReservedInstancesService;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test the construction of {@link ActionApiDTO} with compound moves.
 */
public class CompoundMoveTest {

    private static final long REAL_TIME_TOPOLOGY_CONTEXT_ID = 777777L;

    private ActionSpecMapper mapper;

    private PolicyServiceMole policyMole = spy(PolicyServiceMole.class);

    private SupplyChainServiceMole supplyChainMole = spy(new SupplyChainServiceMole());

    private RepositoryApi repositoryApi;

    private GrpcTestServer grpcServer;

    private PolicyServiceGrpc.PolicyServiceBlockingStub policyService;

    private GrpcTestServer supplyChainGrpcServer;

    private SupplyChainServiceGrpc.SupplyChainServiceBlockingStub supplyChainService;

    private ActionSpecMappingContextFactory actionSpecMappingContextFactory;

    private static final int VM = EntityType.VIRTUAL_MACHINE_VALUE;
    private static final int PM = EntityType.PHYSICAL_MACHINE_VALUE;
    private static final int ST = EntityType.STORAGE_VALUE;

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
    private static final long DATACENTER_ID = 50;
    private static final String DC_NAME = "DC-1";
    private static final long DATACENTER2_ID = 51;
    private static final String DC2_NAME = "DC-2";

    private final ServiceEntityMapper serviceEntityMapper = mock(ServiceEntityMapper.class);

    private final VirtualVolumeAspectMapper virtualVolumeAspectMapper = mock(VirtualVolumeAspectMapper.class);

    @Before
    public void setup() throws Exception {
        final List<PolicyResponse> policyResponses = ImmutableList.of(
            PolicyResponse.newBuilder().setPolicy(Policy.newBuilder()
                .setId(1)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setName("policy")))
                .build());
        when(policyMole.getPolicies(Mockito.any())).thenReturn(policyResponses);
        grpcServer = GrpcTestServer.newServer(policyMole);
        grpcServer.start();
        policyService = PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub riBuyContextFetchServiceStub =
                RIBuyContextFetchServiceGrpc.newBlockingStub(grpcServer.getChannel());

        final List<GetMultiSupplyChainsResponse> supplyChainResponses = ImmutableList.of(
            makeGetMultiSupplyChainResponse(TARGET_ENTITY_ID, DATACENTER_ID),
            makeGetMultiSupplyChainResponse(PM1_ID, DATACENTER_ID),
            makeGetMultiSupplyChainResponse(PM2_ID, DATACENTER2_ID),
            makeGetMultiSupplyChainResponse(ST1_ID, DATACENTER_ID),
            makeGetMultiSupplyChainResponse(ST2_ID, DATACENTER2_ID));
        when(supplyChainMole.getMultiSupplyChains(Mockito.any())).thenReturn(supplyChainResponses);
        supplyChainGrpcServer = GrpcTestServer.newServer(supplyChainMole);
        supplyChainGrpcServer.start();
        supplyChainService = SupplyChainServiceGrpc
            .newBlockingStub(supplyChainGrpcServer.getChannel());

        repositoryApi = mock(RepositoryApi.class);
        final SearchRequest emptySearchReq = ApiTestUtils.mockEmptySearchReq();
        when(repositoryApi.getRegion(any())).thenReturn(emptySearchReq);

        when(virtualVolumeAspectMapper.mapVirtualMachines(anySetOf(Long.class), anyLong())).thenReturn(Collections.emptyMap());
        when(virtualVolumeAspectMapper.mapVirtualVolumes(anySetOf(Long.class), anyLong(), anyBoolean())).thenReturn(Collections.emptyMap());

        actionSpecMappingContextFactory = new ActionSpecMappingContextFactory(policyService,
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().build()), repositoryApi,
                mock(EntityAspectMapper.class), virtualVolumeAspectMapper,
                REAL_TIME_TOPOLOGY_CONTEXT_ID, null, null, serviceEntityMapper,
                supplyChainService, Mockito.mock(PoliciesService.class),
                mock(ReservedInstancesService.class));

        CostServiceGrpc.CostServiceBlockingStub costServiceBlockingStub =
                CostServiceGrpc.newBlockingStub(grpcServer.getChannel());
        ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub
                reservedInstanceUtilizationCoverageServiceBlockingStub =
                ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(grpcServer.getChannel());

        mapper = new ActionSpecMapper(
                actionSpecMappingContextFactory,
                serviceEntityMapper,
                mock(PoliciesService.class),
                mock(ReservedInstanceMapper.class),
                riBuyContextFetchServiceStub,
                costServiceBlockingStub,
                reservedInstanceUtilizationCoverageServiceBlockingStub,
                mock(BuyRiScopeHandler.class),
                REAL_TIME_TOPOLOGY_CONTEXT_ID,
                mock(UuidMapper.class));
        IdentityGenerator.initPrefix(0);

        final MultiEntityRequest multiReq = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity(TARGET_NAME, TARGET_ENTITY_ID, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(PM1_NAME, PM1_ID, EntityType.PHYSICAL_MACHINE_VALUE),
                new TestEntity(PM2_NAME, PM2_ID, EntityType.PHYSICAL_MACHINE_VALUE),
                new TestEntity(ST1_NAME, ST1_ID, EntityType.STORAGE_VALUE),
                new TestEntity(ST2_NAME, ST2_ID, EntityType.STORAGE_VALUE),
                new TestEntity(VOL1_NAME, VOL1_ID, EntityType.VIRTUAL_VOLUME_VALUE),
                new TestEntity(DC_NAME, DATACENTER_ID, EntityType.DATACENTER_VALUE),
                new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE))));

        when(repositoryApi.entitiesRequest(any())).thenReturn(multiReq);
    }

    private GetMultiSupplyChainsResponse makeGetMultiSupplyChainResponse(long entityOid,
                                                                         long dataCenterOid) {
        return GetMultiSupplyChainsResponse.newBuilder().setSeedOid(entityOid).setSupplyChain(SupplyChain
            .newBuilder().addSupplyChainNodes(makeSupplyChainNode(dataCenterOid)))
            .build();
    }

    private SupplyChainNode makeSupplyChainNode(long oid) {
        return SupplyChainNode.newBuilder()
            .setEntityType(ApiEntityType.DATACENTER.typeNumber())
            .putMembersByState(EntityState.ACTIVE.ordinal(),
                MemberList.newBuilder().addMemberOids(oid).build())
            .build();
    }

    /**
     * A Test class to store information about entity, used for testing only.
     */
    final class TestEntity {
        public String displayName;
        public long oid;
        public int entityType;

        TestEntity(String displayName, long oid, int entityType) {
            this.displayName = displayName;
            this.oid = oid;
            this.entityType = entityType;
        }
    }

    private List<ApiPartialEntity> topologyEntityDTOList(List<TestEntity> testEntities) {
        Map<Long, ServiceEntityApiDTO> map = new HashMap<>();

        List<ApiPartialEntity> resp = testEntities.stream().map(testEntity -> {
            final ServiceEntityApiDTO mappedE = new ServiceEntityApiDTO();
            mappedE.setDisplayName(testEntity.displayName);
            mappedE.setUuid(Long.toString(testEntity.oid));
            mappedE.setClassName(ApiEntityType.fromType(testEntity.entityType).apiStr());
            map.put(testEntity.oid, mappedE);

            ApiPartialEntity entity = ApiPartialEntity.newBuilder()
                    .setOid(testEntity.oid)
                    .setDisplayName(testEntity.displayName)
                    .setEntityType(testEntity.entityType)
                    .build();
            when(serviceEntityMapper.toServiceEntityApiDTO(entity)).thenReturn(mappedE);
            return entity;
        }).collect(Collectors.toList());

        when(serviceEntityMapper.toServiceEntityApiDTOMap(any())).thenReturn(map);
        return resp;
    }

    /**
     * Test a compound move with one provider being changed.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSimpleAction() throws Exception {
        ActionDTO.Action moveStorage = makeAction(TARGET_ENTITY_ID, VM, ST1_ID, ST, ST2_ID, ST, null);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveStorage), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(1, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(ST1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(ST2_ID), apiDto.getNewValue());
        assertEquals(DC_NAME, apiDto.getCurrentLocation().getDisplayName());
        assertEquals(DC2_NAME, apiDto.getNewLocation().getDisplayName());
    }

    /**
     * Test a simple move with one provider being changed. This change is for a particular
     * resource of the target.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testSimpleActionWithResource() throws Exception {
        ActionDTO.Action moveVolume = makeAction(TARGET_ENTITY_ID, VM, ST1_ID, ST, ST2_ID, ST, VOL1_ID);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveVolume), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(1, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(ST1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(ST2_ID), apiDto.getNewValue());
    }

    /**
     * Test a compound move with two providers being changed, first
     * is Storage, second is PM.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testCompoundAction1() throws Exception {
        ActionDTO.Action moveBoth1 = makeAction(TARGET_ENTITY_ID, VM, ST1_ID, ST, ST2_ID, ST, PM1_ID, PM, PM2_ID, PM);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveBoth1), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(2, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(PM1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(PM2_ID), apiDto.getNewValue());
    }

    /**
     * Test a compound move with two providers being changed, first
     * is PM, second is Storage.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testCompoundAction2() throws Exception {
        ActionDTO.Action moveBoth2 = makeAction(TARGET_ENTITY_ID, VM,
            PM1_ID, PM, PM2_ID, PM, ST1_ID, ST, ST2_ID, ST); // different order
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveBoth2), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(2, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(PM1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(PM2_ID), apiDto.getNewValue());
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
                        .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder().build()))))
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
                        .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder().build()))))
                        .build();
    }

    private static ActionDTO.Action.Builder genericActionStuff() {
        return ActionDTO.Action.newBuilder()
                        .setId(IdentityGenerator.next())
                        .setDeprecatedImportance(0)
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
}
