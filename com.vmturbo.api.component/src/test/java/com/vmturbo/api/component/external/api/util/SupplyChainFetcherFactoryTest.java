package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.enums.SupplyChainDetailType;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCount;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainMoles.SupplyChainServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;

public class SupplyChainFetcherFactoryTest {
    private static final String VM = "VirtualMachine";
    private static final String PM = "PhysicalMachine";
    private static final long LIVE_TOPOLOGY_ID = 1234;

    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */
    private final EntitySeverityServiceMole severityServiceBackend =
        Mockito.spy(new EntitySeverityServiceMole());
    private final SupplyChainServiceMole supplyChainServiceBackend =
        Mockito.spy(new SupplyChainServiceMole());
    private RepositoryApiMock repositoryApiBackend;
    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(severityServiceBackend,
            supplyChainServiceBackend);

    @Before
    public void setup() throws IOException {

        Duration timeoutDuration = Duration.ofMinutes(1);

        // set up the mockseverity RPC
        EntitySeverityServiceBlockingStub entitySeverityRpc =
                EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
        repositoryApiBackend =
                Mockito.spy(new RepositoryApiMock(entitySeverityRpc));


        // set up the ActionsService under test
        supplyChainFetcherFactory = new SupplyChainFetcherFactory(grpcServer.getChannel(), grpcServer.getChannel(),
                repositoryApiBackend, groupExpander, timeoutDuration, 7);
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
        final SupplyChainNode vms = SupplyChainNode.newBuilder()
            .setEntityType(VM)
            .addMemberOids(1L)
            .build();
        final SupplyChainNode hosts = SupplyChainNode.newBuilder()
            .setEntityType(PM)
            .addMemberOids(5L)
            .build();

        when(supplyChainServiceBackend.getSupplyChain(any()))
                .thenReturn(Arrays.asList(vms, hosts));

        when(severityServiceBackend.getSeverityCounts(MultiEntityRequest.newBuilder()
            .setTopologyContextId(LIVE_TOPOLOGY_ID)
            .addEntityIds(1L)
            .build()))
            .thenReturn(SeverityCountsResponse.newBuilder()
                .addCounts(newSeverityCount(Severity.NORMAL, 1L))
                .build());
        when(severityServiceBackend.getSeverityCounts(MultiEntityRequest.newBuilder()
            .setTopologyContextId(LIVE_TOPOLOGY_ID)
            .addEntityIds(5L)
            .build()))
            .thenReturn(SeverityCountsResponse.newBuilder()
                .addCounts(newSeverityCount(Severity.NORMAL, 1L))
                .build());

        final SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .includeHealthSummary(true)
                .fetch();

        assertThat(result.getSeMap().size(), is(2));
        assertThat(getObjectsCountInHealth(result, VM), is(vms.getMemberOidsList().size()));
        assertThat(getObjectsCountInHealth(result, PM), is(hosts.getMemberOidsList().size()));

        assertThat(getSeveritySize(result, VM, Severity.NORMAL), is(vms.getMemberOidsList().size()));
        assertThat(getSeveritySize(result, PM, Severity.NORMAL), is(hosts.getMemberOidsList().size()));
    }

    @Test
    public void testSupplyChainNodeFetcher() throws Exception {
        final SupplyChainNode vms = SupplyChainNode.newBuilder()
                .setEntityType(VM)
                .addMemberOids(1L)
                .build();
        when(supplyChainServiceBackend.getSupplyChain(SupplyChainRequest.newBuilder()
                .addEntityTypesToInclude(VM)
                .build()))
            .thenReturn(Collections.singletonList(vms));
        final Map<String, SupplyChainNode> nodes = supplyChainFetcherFactory.newNodeFetcher()
                .entityTypes(Collections.singletonList(VM))
                .fetch();
        assertThat(nodes.size(), is(1));
        assertThat(nodes.get(VM), is(vms));
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
            .addMemberOids(1L)
            .addMemberOids(2L)
            .build();
        final SupplyChainNode hosts = SupplyChainNode.newBuilder()
            .setEntityType(PM)
            .addMemberOids(5L)
            .build();
        when(supplyChainServiceBackend.getSupplyChain(any()))
                .thenReturn(Arrays.asList(vms, hosts));

        when(severityServiceBackend.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(LIVE_TOPOLOGY_ID)
                .addEntityIds(1L)
                .addEntityIds(2L)
                .build()))
            .thenReturn(Arrays.asList(newSeverity(1L, Severity.CRITICAL),
                    newSeverity(2L, null)));
        when(severityServiceBackend.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(LIVE_TOPOLOGY_ID)
                .addEntityIds(5L)
                .build()))
            .thenReturn(Collections.singletonList(newSeverity(5L, Severity.MAJOR)));

        repositoryApiBackend.putServiceEnity(1L, VM, Severity.CRITICAL);
        repositoryApiBackend.putServiceEnity(2L, VM, null);
        repositoryApiBackend.putServiceEnity(5L, PM, Severity.MAJOR);

        // act
        final SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .addSeedUuid("Market")
                .includeHealthSummary(true)
                .supplyChainDetailType(SupplyChainDetailType.entity)
                .fetch();

        // assert
        Assert.assertEquals(2, result.getSeMap().size());
        Assert.assertEquals(vms.getMemberOidsList().size(), getObjectsCountInHealth(result, VM));
        Assert.assertEquals(hosts.getMemberOidsList().size(), getObjectsCountInHealth(result, PM));

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

    public static class RepositoryApiMock extends RepositoryApi {

        private static String MOCK_HOSTNAME = "mock-repository";
        private static int MOCK_PORT = 0;
        private static long MOCK_REALTIME_CONTEXT_ID = 123;

        private Map<Long, ServiceEntityApiDTO> seMap = new HashMap<>();

        public RepositoryApiMock(EntitySeverityServiceBlockingStub entitySeverityRpc) {
            super(MOCK_HOSTNAME, MOCK_PORT, Mockito.mock(RestTemplate.class),
                    entitySeverityRpc,
                    MOCK_REALTIME_CONTEXT_ID);
        }

        public void putServiceEnity(long oid, String entityType, Severity severity) {
            seMap.put(oid, createServiceEntityApiDTO(oid, entityType, severity));
        }

        @Override
        public @Nonnull Map<Long, Optional<ServiceEntityApiDTO>> getServiceEntitiesById(
                @Nonnull final ServiceEntitiesRequest serviceEntitiesRequest) {
            return serviceEntitiesRequest.getEntityIds().stream()
                    .collect(Collectors.toMap(oid -> oid,
                            oid -> seMap.containsKey(oid) ? Optional.of(seMap.get(oid)) :
                                    Optional.empty()
                    ));
        }
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

    @Nonnull
    private static SeverityCount newSeverityCount(@Nonnull Severity severity, long count) {
        final SeverityCount.Builder builder = SeverityCount.newBuilder()
            .setSeverity(severity)
            .setEntityCount(count);

        return builder.build();
    }
}
