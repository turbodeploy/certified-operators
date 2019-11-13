package com.vmturbo.api.component.external.api.mapper;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.aspect.CloudAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualMachineAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc.BuyReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.CostMoles.BuyReservedInstanceServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceSpecServiceMole;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test class for ActionSpecMappingContextFactory.
 */
public class ActionSpecMappingContextFactoryTest {

    private final BuyReservedInstanceServiceMole buyRIMole = Mockito.spy(new BuyReservedInstanceServiceMole());

    private final PolicyServiceMole policyMole = Mockito.spy(new PolicyServiceMole());

    private final ReservedInstanceSpecServiceMole riSpecMole = Mockito.spy(new ReservedInstanceSpecServiceMole());

    private final SupplyChainServiceMole supplyChainMole = Mockito.spy(new SupplyChainServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(buyRIMole, policyMole,
            riSpecMole, supplyChainMole);

    /**
     * Test class to test ActionSpecMappingContextFactory::GetBuyRIIdToRIBoughtandRISpec.
     */
    @Test
    public void testGetBuyRIIdToRIBoughtandRISpec() {
        // RI Buy Actions.
        final BuyRI buyRI1 = BuyRI.newBuilder().setBuyRiId(1).build();
        final BuyRI buyRI2 = BuyRI.newBuilder().setBuyRiId(2).build();
        final List<BuyRI> buyRIActions = Lists.newArrayList(buyRI1, buyRI2);

        final GetBuyReservedInstancesByFilterRequest buyRIRequest = GetBuyReservedInstancesByFilterRequest
                .newBuilder().addAllBuyRiId(buyRIActions.stream()
                        .map(a -> a.getBuyRiId()).collect(Collectors.toList())).build();

        final ReservedInstanceBought riBought1 = ReservedInstanceBought.newBuilder().setId(1)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                .setReservedInstanceSpec(1).build()).build();
        final ReservedInstanceBought riBought2 = ReservedInstanceBought.newBuilder().setId(2)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(2).build()).build();
        final List<ReservedInstanceBought> riBoughts = Lists.newArrayList(riBought1, riBought2);

        final GetBuyReservedInstancesByFilterResponse buyRIResponse = GetBuyReservedInstancesByFilterResponse
                .newBuilder().addAllReservedInstanceBoughts(riBoughts).build();

        when(buyRIMole.getBuyReservedInstancesByFilter(buyRIRequest)).thenReturn(buyRIResponse);


        final ReservedInstanceSpec spec1 = ReservedInstanceSpec.newBuilder().setId(1).build();
        final ReservedInstanceSpec spec2 = ReservedInstanceSpec.newBuilder().setId(2).build();
        final List<ReservedInstanceSpec> specs = Lists.newArrayList(spec1, spec2);
        final GetReservedInstanceSpecByIdsRequest ribySpecRequest = GetReservedInstanceSpecByIdsRequest.newBuilder()
                .addAllReservedInstanceSpecIds(specs.stream()
                .map(a -> a.getId()).collect(Collectors.toList()))
                .build();

        // Three specs present but only two will be mapped.
        final GetReservedInstanceSpecByIdsResponse ribySpecResponse = GetReservedInstanceSpecByIdsResponse.newBuilder()
                .addAllReservedInstanceSpec(specs).build();

        when(riSpecMole.getReservedInstanceSpecByIds(ribySpecRequest)).thenReturn(ribySpecResponse);
        final BuyReservedInstanceServiceBlockingStub buyRIServiceClient = BuyReservedInstanceServiceGrpc
                .newBlockingStub(grpcTestServer.getChannel());
        final PolicyServiceBlockingStub policyService = PolicyServiceGrpc
                .newBlockingStub(grpcTestServer.getChannel());
        final ReservedInstanceSpecServiceBlockingStub riSpecService = ReservedInstanceSpecServiceGrpc
                .newBlockingStub(grpcTestServer.getChannel());
        final SupplyChainServiceBlockingStub supplyChainService = SupplyChainServiceGrpc
                .newBlockingStub(grpcTestServer.getChannel());

        final ActionSpecMappingContextFactory actionSpecMappingContextFactory = new
                            ActionSpecMappingContextFactory(policyService,
                            Mockito.mock(ExecutorService.class),
                            Mockito.mock(RepositoryApi.class),
                            Mockito.mock(CloudAspectMapper.class),
                            Mockito.mock(VirtualMachineAspectMapper.class),
                            Mockito.mock(VirtualVolumeAspectMapper.class),
                            777777,
                            buyRIServiceClient, riSpecService,
                            Mockito.mock(ServiceEntityMapper.class),
                            supplyChainService);

        final Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>>
                buyRIIdToRIBoughtandRISpec = actionSpecMappingContextFactory
                .getBuyRIIdToRIBoughtandRISpec(buyRIActions);

        assertFalse(buyRIIdToRIBoughtandRISpec.isEmpty());
        assertEquals(2, buyRIIdToRIBoughtandRISpec.size());
        assertEquals(riBought1, buyRIIdToRIBoughtandRISpec.get(1L).first);
        assertEquals(spec1, buyRIIdToRIBoughtandRISpec.get(1L).second);
        assertEquals(riBought2, buyRIIdToRIBoughtandRISpec.get(2L).first);
        assertEquals(spec2, buyRIIdToRIBoughtandRISpec.get(2L).second);
    }
}

