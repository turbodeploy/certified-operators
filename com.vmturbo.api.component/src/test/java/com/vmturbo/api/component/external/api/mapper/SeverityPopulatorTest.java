package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk.Builder;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceImplBase;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceStub;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit test for {@link SeverityPopulator}.
 */
public class SeverityPopulatorTest {

    private static final long TIMEOUT_SEC = 30;

    private EntitySeverityHandler actionOrchestratorImpl = new EntitySeverityHandler();
    private EntitySeverityServiceStub entitySeverityRpc;
    private final int realtimeTopologyContextId = 0;

    private final ServiceEntityApiDTO vm1 = new ServiceEntityApiDTO();
    private final ServiceEntityApiDTO vm2 = new ServiceEntityApiDTO();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionOrchestratorImpl);

    private SeverityPopulator severityPopulator;

    @Before
    public void setup() throws IOException {
        entitySeverityRpc = EntitySeverityServiceGrpc.newStub(grpcServer.getChannel());
        severityPopulator = new SeverityPopulator(entitySeverityRpc);

        vm1.setUuid("1");
        vm2.setUuid("2");
    }

    @Test
    public void testPopulateSeveritiesCollection() throws Exception {
        List<ServiceEntityApiDTO> entities = Arrays.asList(vm1, vm2);

        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
            severityBuilder(1L).setSeverity(Severity.MAJOR).build(),
            severityBuilder(2L).build()
        ));
        severityPopulator.populate(realtimeTopologyContextId, entities);

        // The call to populate mutates the vmInstances and pmInstances maps in the dto.
        Assert.assertEquals("Major", entities.get(0).getSeverity());
        Assert.assertEquals("Normal", entities.get(1).getSeverity());
    }

    @Test
    public void testPopulateSeveritiesMap() throws Exception {
        final List<ServiceEntityApiDTO> vms = Arrays.asList(vm1, vm2);

        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
                severityBuilder(1L).setSeverity(Severity.MAJOR).build(),
                severityBuilder(2L).build()
        ));
        severityPopulator.populate(realtimeTopologyContextId, vms);

        // The call to populate mutates the vmInstances and pmInstances maps in the dto.
        Assert.assertEquals("Major", vm1.getSeverity());
        Assert.assertEquals("Normal", vm2.getSeverity());
    }

    @Test
    public void testCalculateSeverityWithMixedSeverities1() throws Exception {
        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
                severityBuilder(1L).setSeverity(Severity.MAJOR).build(),
                severityBuilder(2L).build(),
                severityBuilder(3L).setSeverity(Severity.NORMAL).build(),
                severityBuilder(9999L).setSeverity(Severity.CRITICAL).build()
        ));
        final SeverityMap severity = severityPopulator.getSeverityMap(realtimeTopologyContextId,
                Sets.newHashSet(1L, 2L, 3L, 9999L)).get(TIMEOUT_SEC, TimeUnit.SECONDS);
        Assert.assertEquals(Severity.CRITICAL,
                severity.calculateSeverity(Arrays.asList(1L, 2L, 3L, 9999L)));
        Assert.assertEquals(Severity.MAJOR,
                severity.calculateSeverity(Arrays.asList(1L, 2L, 3L)));
        Assert.assertEquals(Severity.NORMAL,
                severity.calculateSeverity(Arrays.asList(2L, 3L)));
        Assert.assertEquals(Severity.NORMAL,
                severity.calculateSeverity(Collections.singletonList(2L)));
    }

    @Test
    public void testCalculateSeverityWithoutOid() throws Exception {
        actionOrchestratorImpl.setSeveritySupplier(Collections::emptyList);
        final SeverityMap severityMap =
                severityPopulator.getSeverityMap(realtimeTopologyContextId, Collections.emptySet())
                        .get(TIMEOUT_SEC, TimeUnit.SECONDS);
        Assert.assertEquals(Severity.NORMAL,
                severityMap.calculateSeverity(Collections.emptyList()));
    }

    @Test
    public void testCalculateSeverityWithAllNormalSeverity() throws Exception {
        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
                severityBuilder(1L).setSeverity(Severity.NORMAL).build(),
                severityBuilder(2L).build(),
                severityBuilder(3L).setSeverity(Severity.NORMAL).build()));
        final SeverityMap severityMap = severityPopulator.getSeverityMap(realtimeTopologyContextId,
                Sets.newHashSet(1L, 2L, 3L)).get(TIMEOUT_SEC, TimeUnit.SECONDS);
        Assert.assertEquals(Severity.NORMAL,
                severityMap.calculateSeverity(Arrays.asList(1L, 2L, 3L)));
    }


    private EntitySeverity.Builder severityBuilder(final long id) {
        return EntitySeverity.newBuilder()
            .setEntityId(id);
    }

    private class EntitySeverityHandler extends EntitySeverityServiceImplBase {

        private Supplier<List<EntitySeverity>> severitySupplier;

        @Override
        public void getEntitySeverities(MultiEntityRequest request,
                                        StreamObserver<EntitySeveritiesResponse> responseObserver) {
            // If this check is triggered, your test did not define the required supplier method
            // that this class uses. It's a bit dirty but provides nice ergonomics for mocking
            // out the response from the AO RPC calls.
            Objects.requireNonNull(severitySupplier);
            Builder chunks = EntitySeveritiesChunk.newBuilder();
            severitySupplier.get().forEach(entitySeverity -> {
                chunks.addEntitySeverity(entitySeverity);
            });
            responseObserver.onNext(EntitySeveritiesResponse.newBuilder().setEntitySeverity(chunks).build());
            responseObserver.onCompleted();
        }

        /**
         * This lambda should be given an implementation in any tests that call
         * actionOrchestratorImpl#getEntitySeverities.
         */
        public void setSeveritySupplier(Supplier<List<EntitySeverity>> severitySupplier) {
            this.severitySupplier = severitySupplier;
        }
    }
}
