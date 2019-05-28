package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.common.protobuf.topology.UIEntityState;

public class SupplyChainFetcherFactoryTest {
    private static final String VM = "VirtualMachine";
    private static final String PM = "PhysicalMachine";
    private static final long LIVE_TOPOLOGY_ID = 1234;

    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final EntitySeverityServiceMole severityServiceBackend =
        Mockito.spy(new EntitySeverityServiceMole());
    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */

    private final SupplyChainServiceMole supplyChainServiceBackend =
        Mockito.spy(new SupplyChainServiceMole());

    private RepositoryApi repositoryApiBackend = mock(RepositoryApi.class);
    private GroupExpander groupExpander = mock(GroupExpander.class);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(supplyChainServiceBackend, severityServiceBackend);

    @Before
    public void setup() throws IOException {

        // set up the ActionsService under test
        supplyChainFetcherFactory = new SupplyChainFetcherFactory(grpcServer.getChannel(), grpcServer.getChannel(),
                repositoryApiBackend, groupExpander, 7);
    }

    /**
     * Fetch of an OID that isn't known and isn't the distinguished "Market" returns an empty
     * supplychain.
     *
     * @throws Exception should never happen in this test
     */
    @Test
    public void testEmptyGroupExpansion() throws Exception {
        // arrange
        final ImmutableList<String> supplyChainSeedUuids = ImmutableList.of("x");
        final Set<String> supplyChainSeedUuidSet = Sets.newHashSet(supplyChainSeedUuids);

        // act
        SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .addSeedUuids(supplyChainSeedUuidSet)
                .fetch();

        // assert
        assertThat(result.getSeMap(), notNullValue());
        assertThat(result.getSeMap().size(), equalTo(0));
    }

    /**
     * Tests health status consistency for all the entities reported as normal.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testHealthStatusAllNormal() throws Exception {
        final String onState = UIEntityState.ACTIVE.apiStr();
        final String offState = UIEntityState.IDLE.apiStr();
        final SupplyChainNode vms = SupplyChainNode.newBuilder()
            .setEntityType(VM)
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                .addMemberOids(1L)
                .addMemberOids(2L)
                .build())
            .putMembersByState(EntityState.POWERED_OFF_VALUE, MemberList.newBuilder()
                .addMemberOids(3L)
                .build())
            .build();

        when(supplyChainServiceBackend.getSupplyChain(any()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(SupplyChain.newBuilder()
                    .addSupplyChainNodes(vms))
                .build());

        final SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .includeHealthSummary(false)
                .fetch();

        assertThat(result.getSeMap().size(), is(1));
        final Map<String, Integer> stateSummary = result.getSeMap().get(VM).getStateSummary();
        assertNotNull(stateSummary);
        assertThat(stateSummary.keySet(), containsInAnyOrder(onState, offState));
        assertThat(stateSummary.get(onState), is(2));
        assertThat(stateSummary.get(offState), is(1));
    }

    /**
     * Tests proper extraction of entity ids from a correct result.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testFetchIds() throws Exception {
        final SupplyChainNode supplyChainNode1 =
            SupplyChainNode.newBuilder()
                .putMembersByState(
                    EntityState.POWERED_ON_VALUE,
                    MemberList.newBuilder().addMemberOids(1L).addMemberOids(2L).build())
                .putMembersByState(
                    EntityState.POWERED_OFF_VALUE,
                    MemberList.newBuilder().addMemberOids(3L).build())
                .build();
        final SupplyChainNode supplyChainNode2 =
                SupplyChainNode.newBuilder()
                    .putMembersByState(
                        EntityState.POWERED_ON_VALUE,
                        MemberList.newBuilder().addMemberOids(1L).addMemberOids(4L).build())
                    .putMembersByState(
                        EntityState.POWERED_OFF_VALUE,
                        MemberList.newBuilder().addMemberOids(4L).addMemberOids(5L).build())
                    .build();

        when(supplyChainServiceBackend.getSupplyChain(any()))
            .thenReturn(
                GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(
                        SupplyChain.newBuilder()
                            .addSupplyChainNodes(supplyChainNode1)
                            .addSupplyChainNodes(supplyChainNode2)
                            .build())
                    .build());

        Assert.assertEquals(
            LongStream.range(1L, 6L).boxed().collect(Collectors.toSet()),
            supplyChainFetcherFactory.newNodeFetcher().fetchEntityIds());
    }

    @Test
    public void testSupplyChainStateSummary() throws Exception {
    }

    @Test
    public void testSupplyChainNodeFetcher() throws Exception {
        final SupplyChainNode vms = SupplyChainNode.newBuilder()
                .setEntityType(VM)
                .putMembersByState(EntityState.POWERED_ON.getNumber(),
                        MemberList.newBuilder().addMemberOids(1L).build())
                .build();
        when(supplyChainServiceBackend.getSupplyChain(GetSupplyChainRequest.newBuilder()
                .addEntityTypesToInclude(VM)
                .setEnforceUserScope(true)
                .build()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(SupplyChain.newBuilder()
                    .addSupplyChainNodes(vms))
                .build());
        final Map<String, SupplyChainNode> nodes = supplyChainFetcherFactory.newNodeFetcher()
                .entityTypes(Collections.singletonList(VM))
                .fetch();
        assertThat(nodes.size(), is(1));
        assertThat(nodes.get(VM), is(vms));
        assertThat(nodes.get(VM).containsMembersByState(EntityState.POWERED_ON.getNumber()), is(true));
    }

    /**
     * Tests health status consistency for different status of entities.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testHealthStatusWithCritical() throws Exception {
        // arrange
        final SupplyChainNode vms = SupplyChainNode.newBuilder()
            .setEntityType(VM)
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                    .addMemberOids(1L)
                    .addMemberOids(2L)
                    .build())
            .build();
        final SupplyChainNode hosts = SupplyChainNode.newBuilder()
            .setEntityType(PM)
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                    .addMemberOids(5L)
                    .build())
            .build();
        when(supplyChainServiceBackend.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(SupplyChain.newBuilder()
                        .addSupplyChainNodes(vms)
                        .addSupplyChainNodes(hosts))
                    .build());

        when(severityServiceBackend.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(LIVE_TOPOLOGY_ID)
                .addEntityIds(1L)
                .addEntityIds(2L)
                .build()))
            .thenReturn(EntitySeveritiesResponse.newBuilder()
                    .addAllEntitySeverity(
                            Arrays.asList(newSeverity(1L, Severity.CRITICAL),
                                    newSeverity(2L, null)))
                    .build());
        when(severityServiceBackend.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(LIVE_TOPOLOGY_ID)
                .addEntityIds(5L)
                .build()))
            .thenReturn(EntitySeveritiesResponse.newBuilder()
                    .addAllEntitySeverity(Collections.singletonList(newSeverity(5L, Severity.MAJOR)))
                    .build());

        doAnswer(invocation -> {
            ServiceEntitiesRequest req = invocation.getArgumentAt(0, ServiceEntitiesRequest.class);
            final Map<Long, Optional<ServiceEntityApiDTO>> retMap = new HashMap<>();
            if (req.getEntityIds().equals(ImmutableSet.of(1L, 2L))) {
                retMap.put(1L, Optional.of(createServiceEntityApiDTO(1L, VM, Severity.CRITICAL)));
                retMap.put(2L, Optional.of(createServiceEntityApiDTO(2L, VM, null)));
            } else if (req.getEntityIds().equals(ImmutableSet.of(5L))) {
                retMap.put(5L, Optional.of(createServiceEntityApiDTO(5L, PM, Severity.MAJOR)));
            } else {
                return Collections.emptyMap();
            }
            return retMap;
        }).when(repositoryApiBackend).getServiceEntitiesById(any());

        // act
        final SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .addSeedUuid("Market")
                .includeHealthSummary(true)
                .entityDetailType(EntityDetailType.entity)
                .fetch();

        // assert
        Assert.assertEquals(2, result.getSeMap().size());
        Assert.assertEquals(RepositoryDTOUtil.getMemberCount(vms), getObjectsCountInHealth(result, VM));
        Assert.assertEquals(RepositoryDTOUtil.getMemberCount(hosts), getObjectsCountInHealth(result, PM));

        Assert.assertEquals(1, getSeveritySize(result, VM, Severity.CRITICAL));
        Assert.assertEquals(1, getSeveritySize(result, VM, Severity.NORMAL));

        Assert.assertEquals(1, getSeveritySize(result, PM, Severity.MAJOR));

        // test that the SE's returned are populated with the severity info
        Assert.assertEquals(Severity.CRITICAL.name(),
                result.getSeMap().get(VM).getInstances().get("1").getSeverity());
        Assert.assertEquals(Severity.NORMAL.name(), // by default
                result.getSeMap().get(VM).getInstances().get("2").getSeverity());
        Assert.assertEquals(Severity.MAJOR.name(),
                result.getSeMap().get(PM).getInstances().get("5").getSeverity());
    }

    private int getSeveritySize(@Nonnull final SupplychainApiDTO src, @Nonnull String objType,
                                @Nonnull final Severity severity) {
        final String severityStr = ActionDTOUtil.getSeverityName(severity);
        return src.getSeMap().get(objType).getHealthSummary().get(severityStr);
    }

    private int getObjectsCountInHealth(SupplychainApiDTO src, String objType) {
        return src.getSeMap()
            .get(objType)
            .getHealthSummary()
            .values()
            .stream()
            .mapToInt(Integer::intValue)
            .sum();
    }

    private static ServiceEntityApiDTO createServiceEntityApiDTO(long id, String entityType,
                                                                 @Nullable Severity severity) {
        ServiceEntityApiDTO answer = new ServiceEntityApiDTO();
        answer.setUuid(Long.toString(id));
        answer.setClassName(entityType);
        if (severity != null) {
            answer.setSeverity(severity.name());
        }
        return answer;
    }

    @Nonnull
    private static EntitySeverity newSeverity(final long entityId, @Nullable Severity severity) {
        final EntitySeverity.Builder builder = EntitySeverity.newBuilder()
                .setEntityId(entityId);
        if (severity != null) {
            builder.setSeverity(severity);
        }
        return builder.build();
    }
}
