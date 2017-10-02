package com.vmturbo.api.component.external.api.util;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.SupplychainApiDTO;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
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

    @Before
    public void setup() throws IOException {

        // set up a mock Actions RPC server
        GrpcTestServer grpcServer = GrpcTestServer.withServices(severityServiceBackend,
                supplyChainServiceBackend);
        RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
        Duration timeoutDuration = Duration.ofMinutes(1);

        // set up the ActionsService to test
        supplyChainFetcher = new SupplyChainFetcher(grpcServer.getChannel(), grpcServer.getChannel(),
                repositoryApi, timeoutDuration);
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

        final SupplychainApiDTO result = supplyChainFetcher.newBuilder()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .entityTypes(Lists.newArrayList("Market"))
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

        final SupplychainApiDTO result = supplyChainFetcher.newBuilder()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .seedUuid("Market")
                .includeHealthSummary(true)
                .fetch();

        Assert.assertEquals(2, result.getSeMap().size());
        Assert.assertEquals(vms.getMemberOidsList().size(), getObjectsCountInHealth(result, VM));
        Assert.assertEquals(hosts.getMemberOidsList().size(), getObjectsCountInHealth(result, PM));

        Assert.assertEquals(1, getSeveritySize(result, VM, Severity.CRITICAL));
        Assert.assertEquals(2, getSeveritySize(result, VM, Severity.MAJOR));
        Assert.assertEquals(1, getSeveritySize(result, VM, Severity.NORMAL));

        Assert.assertEquals(1, getSeveritySize(result, PM, Severity.MAJOR));
        Assert.assertEquals(2, getSeveritySize(result, PM, Severity.NORMAL));
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
}