package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.enums.SupplyChainDetailType;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceImplBase;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;

public class SupplyChainFetcherTest {
    private static final String VM = "VirtualMachine";
    private static final String PM = "PhysicalMachine";
    private static final long LIVE_TOPOLOGY_ID = 1234;

    private SupplyChainFetcher supplyChainFetcher;

    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */
    private final EntitySeverityServiceMock severityServiceBackend =
        Mockito.spy(new EntitySeverityServiceMock());
    private final SupplyChainServiceMock supplyChainServiceBackend =
        Mockito.spy(new SupplyChainServiceMock());
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
        supplyChainFetcher = new SupplyChainFetcher(grpcServer.getChannel(), grpcServer.getChannel(),
                repositoryApiBackend, groupExpander, timeoutDuration);
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
        SupplychainApiDTO result = supplyChainFetcher.newOperation()
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
            .addAllMemberOids(Arrays.asList(1L, 2L, 3L, 4L))
            .build();
        final SupplyChainNode hosts = SupplyChainNode.newBuilder()
            .setEntityType(PM)
            .addAllMemberOids(Arrays.asList(5L, 6L, 7L))
            .build();
        supplyChainServiceBackend.addNode(vms);
        supplyChainServiceBackend.addNode(hosts);

        final SupplychainApiDTO result = supplyChainFetcher.newOperation()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .includeHealthSummary(true)
                .fetch();

        Assert.assertEquals(2, result.getSeMap().size());
        Assert.assertEquals(vms.getMemberOidsList().size(), getObjectsCountInHealth(result, VM));
        Assert.assertEquals(hosts.getMemberOidsList().size(), getObjectsCountInHealth(result, PM));

        Assert.assertEquals(vms.getMemberOidsList().size(), getSeveritySize(result, VM, Severity.NORMAL));
        Assert.assertEquals(hosts.getMemberOidsList().size(),
            getSeveritySize(result, PM, Severity.NORMAL));
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
            .addAllMemberOids(Arrays.asList(1L, 2L, 3L, 4L))
            .build();
        final SupplyChainNode hosts = SupplyChainNode.newBuilder()
            .setEntityType(PM)
            .addAllMemberOids(Arrays.asList(5L, 6L, 7L))
            .build();
        supplyChainServiceBackend.addNode(vms);
        supplyChainServiceBackend.addNode(hosts);

        severityServiceBackend.putSeverity(1L, Severity.CRITICAL);
        severityServiceBackend.putSeverity(2L, Severity.MAJOR);
        severityServiceBackend.putSeverity(3L, Severity.MAJOR);
        severityServiceBackend.putSeverity(6L, Severity.MAJOR);

        repositoryApiBackend.putServiceEnity(1L, VM, Severity.CRITICAL);
        repositoryApiBackend.putServiceEnity(2L, VM, Severity.MAJOR);
        repositoryApiBackend.putServiceEnity(3L, VM, Severity.MAJOR);
        repositoryApiBackend.putServiceEnity(4L, VM, null);
        repositoryApiBackend.putServiceEnity(5L, PM, null);
        repositoryApiBackend.putServiceEnity(6L, PM, Severity.MAJOR);
        repositoryApiBackend.putServiceEnity(7L, PM, null);

        // act
        final SupplychainApiDTO result = supplyChainFetcher.newOperation()
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
        Assert.assertEquals(2, getSeveritySize(result, VM, Severity.MAJOR));
        Assert.assertEquals(1, getSeveritySize(result, VM, Severity.NORMAL));

        Assert.assertEquals(1, getSeveritySize(result, PM, Severity.MAJOR));
        Assert.assertEquals(2, getSeveritySize(result, PM, Severity.NORMAL));

        // test that the SE's returned are populated with the severity info
        Assert.assertEquals(Severity.CRITICAL.name(),
                result.getSeMap().get(VM).getInstances().get("1").getSeverity());
        Assert.assertEquals(Severity.MAJOR.name(),
                result.getSeMap().get(VM).getInstances().get("2").getSeverity());
        Assert.assertEquals(Severity.MAJOR.name(),
                result.getSeMap().get(VM).getInstances().get("3").getSeverity());
        Assert.assertEquals(Severity.NORMAL.name(),   // ... by default
                result.getSeMap().get(VM).getInstances().get("4").getSeverity());
        Assert.assertEquals(Severity.NORMAL.name(),   // ... by default
                result.getSeMap().get(PM).getInstances().get("5").getSeverity());
        Assert.assertEquals(Severity.MAJOR.name(),
                result.getSeMap().get(PM).getInstances().get("6").getSeverity());
        Assert.assertEquals(Severity.NORMAL.name(),   // ... by default
                result.getSeMap().get(PM).getInstances().get("7").getSeverity());
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

    public static class SupplyChainServiceMock extends SupplyChainServiceImplBase {
        private final List<SupplyChainNode> nodes = new ArrayList<>();

        void addNode(@Nonnull final SupplyChainNode node) {
            nodes.add(Objects.requireNonNull(node));
        }

        @Override
        public void getSupplyChain(SupplyChainRequest request,
                                   StreamObserver<SupplyChainNode> responseObserver) {
            nodes.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }

    /**
     * Entity service gRPC server mock. Used to report severities for different entities.
     */
    public static class EntitySeverityServiceMock extends EntitySeverityServiceImplBase {

        private Map<Long, Severity> severities = new HashMap<>();

        void putSeverity(@Nonnull final Long oid, @Nonnull final Severity severity) {
            severities.put(oid, severity);
        }

        @Override
        public void getEntitySeverities(MultiEntityRequest request,
                                        StreamObserver<EntitySeverity> responseObserver) {
            for (Long id : request.getEntityIdsList()) {
                EntitySeverity.Builder severityBuilder = EntitySeverity.newBuilder()
                    .setEntityId(id);

                final Severity severity = severities.get(id);
                if (severity != null) {
                    severityBuilder.setSeverity(severity);
                }

                responseObserver.onNext(severityBuilder.build());
            }
            responseObserver.onCompleted();
        }
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
        public @Nonnull Map<Long, Optional<ServiceEntityApiDTO>>getServiceEntitiesById(
                @Nonnull Set<Long> memberOidsList) {
            return memberOidsList.stream()
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
}
