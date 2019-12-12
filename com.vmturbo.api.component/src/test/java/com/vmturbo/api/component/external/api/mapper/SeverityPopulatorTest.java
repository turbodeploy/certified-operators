package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk.Builder;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;

public class SeverityPopulatorTest {
    private EntitySeverityHandler actionOrchestratorImpl = new EntitySeverityHandler();
    private EntitySeverityServiceBlockingStub entitySeverityRpc;
    private final int realtimeTopologyContextId = 0;

    private final ServiceEntityApiDTO vm1 = new ServiceEntityApiDTO();
    private final ServiceEntityApiDTO vm2 = new ServiceEntityApiDTO();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionOrchestratorImpl);

    private SeverityPopulator severityPopulator;

    @Before
    public void setup() throws IOException {
        entitySeverityRpc = EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
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
        final Map<Long, ServiceEntityApiDTO> entityDTOs = ImmutableMap.of(
            Long.parseLong(vm1.getUuid()), vm1,
            Long.parseLong(vm2.getUuid()), vm2
        );

        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
            severityBuilder(1L).setSeverity(Severity.MAJOR).build(),
            severityBuilder(2L).build()
        ));
        severityPopulator.populate(realtimeTopologyContextId, entityDTOs);

        // The call to populate mutates the vmInstances and pmInstances maps in the dto.
        Assert.assertEquals("Major", vm1.getSeverity());
        Assert.assertEquals("Normal", vm2.getSeverity());
    }

    @Test
    public void testPopulateSeveritiesSupplyChain() throws Exception {
        Map<String, ServiceEntityApiDTO> vmInstances = new HashMap<>();
        vmInstances.put("1", vm1);
        vmInstances.put("2", vm2);

        Map<String, ServiceEntityApiDTO> pmInstances = new HashMap<>();
        pmInstances.put("9999", new ServiceEntityApiDTO());

        SupplychainEntryDTO vmEntry = new SupplychainEntryDTO();
        vmEntry.setInstances(vmInstances);

        SupplychainEntryDTO pmEntry = new SupplychainEntryDTO();
        pmEntry.setInstances(pmInstances);

        Map<String, SupplychainEntryDTO> seMap = new HashMap<>();
        seMap.put("VirtualMachine", vmEntry);
        seMap.put("PhysicalMachine", pmEntry);

        SupplychainApiDTO dto = new SupplychainApiDTO();
        dto.setSeMap(seMap);

        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
            severityBuilder(1L).setSeverity(Severity.MAJOR).build(),
            severityBuilder(2L).build(),
            severityBuilder(9999L).setSeverity(Severity.CRITICAL).build()
        ));
        severityPopulator.populate(realtimeTopologyContextId, dto);

        // The call to populate mutates the vmInstances and pmInstances maps in the dto.
        Assert.assertEquals("Major", vmInstances.get("1").getSeverity());
        Assert.assertEquals("Normal", vmInstances.get("2").getSeverity());
        Assert.assertEquals("Critical", pmInstances.get("9999").getSeverity());
    }


    @Test
    public void testCalculateSeverityWithMixedSeverities1() {
        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
                severityBuilder(1L).setSeverity(Severity.MAJOR).build(),
                severityBuilder(2L).build(),
                severityBuilder(3L).setSeverity(Severity.NORMAL).build(),
                severityBuilder(9999L).setSeverity(Severity.CRITICAL).build()
        ));
        Optional<Severity> severity = severityPopulator
                .calculateSeverity(realtimeTopologyContextId,
                        Sets.newHashSet(1L, 2L, 3L, 9999L));
        Assert.assertEquals("CRITICAL", severity.get().name());
    }

    @Test
    public void testCalculateSeverityWithMixedSeverities2() {
        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
                severityBuilder(1L).setSeverity(Severity.MAJOR).build(),
                severityBuilder(2L).build(),
                severityBuilder(3L).setSeverity(Severity.NORMAL).build(),
                severityBuilder(9999L).setSeverity(Severity.MINOR).build()
        ));
        Optional<Severity> severity = severityPopulator
                .calculateSeverity(realtimeTopologyContextId,
                        Sets.newHashSet(1L, 2L, 3L, 9999L));
        Assert.assertEquals("MAJOR", severity.get().name());
    }

    @Test
    public void testCalculateSeverityWithoutOid() {
        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of());
        Optional<Severity> severity = severityPopulator
                .calculateSeverity(realtimeTopologyContextId,
                        ImmutableSet.of());
        Assert.assertEquals("NORMAL", severity.get().name());
    }

    @Test
    public void testCalculateSeverityWithAllNormalSeverity() {
        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
                severityBuilder(1L).setSeverity(Severity.NORMAL).build(),
                severityBuilder(2L).build(),
                severityBuilder(3L).setSeverity(Severity.NORMAL).build()
        ));
        Optional<Severity> severity = severityPopulator
                .calculateSeverity(realtimeTopologyContextId,
                        Sets.newHashSet(1L, 2L, 3L));
        Assert.assertEquals("NORMAL", severity.get().name());
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
