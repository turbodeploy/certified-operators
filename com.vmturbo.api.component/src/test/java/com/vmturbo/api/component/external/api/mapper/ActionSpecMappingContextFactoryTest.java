package com.vmturbo.api.component.external.api.mapper;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.util.JsonFormat;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMappingContextFactory.ActionSpecMappingContext;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
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
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceSpecServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Test class for ActionSpecMappingContextFactory.
 */
public class ActionSpecMappingContextFactoryTest {

    private final BuyReservedInstanceServiceMole buyRIMole = Mockito.spy(new BuyReservedInstanceServiceMole());

    private final PolicyServiceMole policyMole = Mockito.spy(new PolicyServiceMole());

    private final ReservedInstanceSpecServiceMole riSpecMole = Mockito.spy(new ReservedInstanceSpecServiceMole());

    private final SupplyChainServiceMole supplyChainMole = Mockito.spy(new SupplyChainServiceMole());

    private final CostServiceMole costServiceMole = Mockito.spy(new CostServiceMole());

    /**
     * Test server for stubbed services.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(buyRIMole, policyMole,
            riSpecMole, supplyChainMole, costServiceMole);

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
                            Mockito.mock(EntityAspectMapper.class),
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

    /**
     * Class to test ActionSpecMappingContextFactory::createActionSpecMappingContext.
     */
    @Test
    public void createActionSpecMappingContext() {
        final BuyReservedInstanceServiceBlockingStub buyRIServiceClient = BuyReservedInstanceServiceGrpc
            .newBlockingStub(grpcTestServer.getChannel());
        final PolicyServiceBlockingStub policyService = PolicyServiceGrpc
            .newBlockingStub(grpcTestServer.getChannel());
        final ReservedInstanceSpecServiceBlockingStub riSpecService = ReservedInstanceSpecServiceGrpc
            .newBlockingStub(grpcTestServer.getChannel());
        final SupplyChainServiceBlockingStub supplyChainService = SupplyChainServiceGrpc
            .newBlockingStub(grpcTestServer.getChannel());
        final CostServiceBlockingStub costService = CostServiceGrpc
            .newBlockingStub(grpcTestServer.getChannel());

        /*
         * Load test scenario.
         */
        final List<Action> actions = new ArrayList<>();
        final Map<Long, ApiPartialEntity> regions = new HashMap<>();
        final Map<Long, ApiPartialEntity> entities = new HashMap<>();
        final Map<Long, ApiPartialEntity> entitiesById = new HashMap<>();
        final Map<Long, ApiPartialEntity> entityIdToRegion = new HashMap<>();
        final Map<Long, ApiPartialEntity> entitiesAfter = new HashMap<>();
        final Map<Long, ApiPartialEntity> entitiesByIdAfter = new HashMap<>();
        final List<Map<Long, ApiPartialEntity>> maps = new ArrayList<>(
            Arrays.asList(regions, entities, entitiesById, entityIdToRegion,
                            entitiesAfter, entitiesByIdAfter)
        );
        InputStream is = ActionSpecMappingContextFactory.class
            .getResourceAsStream("/input-data.json");
        String line = "";
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            int mapNumber = -2;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("====")) {
                    mapNumber++;
                    continue;
                }
                if (mapNumber < 0) {
                    Action.Builder builder = Action.newBuilder();
                    JsonFormat.parser().merge(line, builder);
                    actions.add(builder.build());
                } else {
                    ApiPartialEntity.Builder builder = ApiPartialEntity.newBuilder();
                    // Split line into key/value pair if a separate key is present.
                    Long key = null;
                    if (!line.startsWith("{")) {
                        String[] kv = line.split(" ", 2);
                        key = Long.parseLong(kv[0]);
                        line = kv[1];
                    }
                    JsonFormat.parser().merge(line, builder);
                    ApiPartialEntity ape = builder.build();
                    maps.get(mapNumber).put(key == null ? ape.getOid() : key, ape);
                }
            }
        } catch (IOException e) {
            System.out.println("Error parsing line: " + line);
            e.printStackTrace();
        }

        // repositoryApi.entitiesRequest(srcEntities)
        RepositoryApi repositoryApiMock = Mockito.mock(RepositoryApi.class);
        MultiEntityRequest merEntities = Mockito.mock(MultiEntityRequest.class);
        when(merEntities.getEntities()).thenReturn(entities.values().stream());
        when(merEntities.contextId(any())).thenReturn(merEntities);
        when(repositoryApiMock.entitiesRequest(any())).thenReturn(merEntities);

        SearchRequest srRegions = Mockito.mock(SearchRequest.class);
        when(srRegions.getEntities()).thenReturn(regions.values().stream());
        when(repositoryApiMock.getRegion(any())).thenReturn(srRegions);


        SupplyChainServiceBlockingStub supplyChainRpc = SupplyChainServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        ThinTargetCache thinTargetCache = Mockito.mock(ThinTargetCache.class);
        Mockito.when(thinTargetCache.getTargetInfo(Mockito.anyLong()))
            .thenAnswer(invocation -> Optional.empty());
        ServiceEntityMapper serviceEntityMapper = new ServiceEntityMapper(
                        thinTargetCache,
                        costService, supplyChainRpc);

        final ActionSpecMappingContextFactory actionSpecMappingContextFactory = new
            ActionSpecMappingContextFactory(policyService,
            MoreExecutors.newDirectExecutorService(),
            repositoryApiMock,
            Mockito.mock(EntityAspectMapper.class),
            Mockito.mock(VirtualVolumeAspectMapper.class),
            777777,
            buyRIServiceClient, riSpecService,
            serviceEntityMapper,
            supplyChainService);

        long topologyContextId = 777777L;
        ActionSpecMappingContext result = null;
        try {
            result = actionSpecMappingContextFactory
                .createActionSpecMappingContext(actions, topologyContextId);
        } catch (Exception e) {
            e.printStackTrace();  // Let test framework deal with it.
        }
        assertNotNull(result);
        assertEquals(111, result.getServiceEntityApiDTOs().size());
        Long[] regionOids = {
                73367284550293L, 73367279135652L, 73385266467456L, 73367284550294L, 73367284550742L,
                73367279135905L, 73367284550620L, 73367284550619L, 73367279135670L, 73367284550213L,
                73367279135862L, 73367279135543L, 73367284550855L, 73367357167082L, 73367279135861L,
                73367279135602L, 73367284550403L, 73367279251058L, 73367357167086L, 73367284550669L,
                73367279496768L, 73367279135674L, 73367279135547L, 73367279846082L, 73367279846073L,
                73367280089204L, 73385034588144L, 73367284550387L, 73367284550194L, 73367284550706L,
                73367284550013L, 73367279135566L, 73367279135882L, 73367279135818L, 73367284550651L,
                73367279135561L, 73367279135625L, 73367284550437L, 73367284550436L, 73367279135572L,
                73367279135571L, 73367357167052L, 73367284550368L, 73367280089256L, 73367357166981L,
                73367279846050L, 73367279135833L, 73367284550442L};
        for (Long oid : regionOids) {
            assertNotNull(result.getRegion(oid));
        }
    }
}

