package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
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

    @Before
    public void setup() throws IOException {
        entitySeverityRpc = EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());

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
        SeverityPopulator.populate(entitySeverityRpc, realtimeTopologyContextId, entities);

        // The call to populate mutates the vmInstances and pmInstances maps in the dto.
        Assert.assertEquals("Major", entities.get(0).getSeverity());
        Assert.assertEquals("Normal", entities.get(1).getSeverity());
    }

    @Test
    public void testPopulateSeveritiesMap() throws Exception {
        final Map<Long, Optional<ServiceEntityApiDTO>> entityDTOs = ImmutableMap.of(
            Long.parseLong(vm1.getUuid()), Optional.of(vm1),
            Long.parseLong(vm2.getUuid()), Optional.of(vm2)
        );

        actionOrchestratorImpl.setSeveritySupplier(() -> ImmutableList.of(
            severityBuilder(1L).setSeverity(Severity.MAJOR).build(),
            severityBuilder(2L).build()
        ));
        SeverityPopulator.populate(entitySeverityRpc, realtimeTopologyContextId, entityDTOs);

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
        SeverityPopulator.populate(entitySeverityRpc, realtimeTopologyContextId, dto);

        // The call to populate mutates the vmInstances and pmInstances maps in the dto.
        Assert.assertEquals("Major", vmInstances.get("1").getSeverity());
        Assert.assertEquals("Normal", vmInstances.get("2").getSeverity());
        Assert.assertEquals("Critical", pmInstances.get("9999").getSeverity());
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
            EntitySeveritiesResponse.Builder responseBuilder = EntitySeveritiesResponse.newBuilder();
            severitySupplier.get().forEach(responseBuilder::addEntitySeverity);
            responseObserver.onNext(responseBuilder.build());
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
