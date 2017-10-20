package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecByEntityTypeRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TemplateSpecRpcServiceTest {

    private TemplateSpecParser templateSpecParser = Mockito.mock(TemplateSpecParser.class);

    private TemplateSpecRpcService templateSpecRpcService =
            new TemplateSpecRpcService(templateSpecParser);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templateSpecRpcService);

    private TemplateSpecServiceBlockingStub templateSpecServiceBlockingStub;

    @Before
    public void init() throws Exception {
        templateSpecServiceBlockingStub = TemplateSpecServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    @Test
    public void testGetTemplateSpec() {
        TemplateSpec testVMTemplateSpec = TemplateSpec.newBuilder()
            .setId(1)
            .setName("testVM")
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .build();
        Map<String, TemplateSpec> templateSpecMap = new HashMap<>();
        templateSpecMap.put(EntityType.VIRTUAL_MACHINE.toString(), testVMTemplateSpec);
        Mockito.when(templateSpecParser.getTemplateSpecMap()).thenReturn(templateSpecMap);
        final GetTemplateSpecRequest request = GetTemplateSpecRequest.newBuilder()
            .setId(1)
            .build();
        final TemplateSpec templateSpec = templateSpecServiceBlockingStub.getTemplateSpec(request);
        assertEquals(templateSpec, testVMTemplateSpec);
    }

    @Test
    public void testGetAllTemplateSpecs() {
        TemplateSpec testVMTemplateSpec = TemplateSpec.newBuilder()
            .setId(1)
            .setName("testVM")
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .build();
        TemplateSpec testPMTemplateSpec = TemplateSpec.newBuilder()
            .setId(2)
            .setName("testPM")
            .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
            .build();
        Map<String, TemplateSpec> templateSpecMap = new HashMap<>();
        templateSpecMap.put(EntityType.VIRTUAL_MACHINE.toString(), testVMTemplateSpec);
        templateSpecMap.put(EntityType.PHYSICAL_MACHINE.toString(), testPMTemplateSpec);
        Mockito.when(templateSpecParser.getTemplateSpecMap()).thenReturn(templateSpecMap);
        final GetTemplateSpecsRequest request = GetTemplateSpecsRequest.getDefaultInstance();
        final Iterator<TemplateSpec> templateSpec = templateSpecServiceBlockingStub.getTemplateSpecs(request);
        final Iterable<TemplateSpec> templateSpecs = () -> templateSpec;
        final Set<TemplateSpec> templateSpecSet = StreamSupport.stream(templateSpecs.spliterator(), false)
            .collect(Collectors.toSet());
        assertEquals(templateSpecSet.size(), 2);
        assertTrue(templateSpecSet.contains(testVMTemplateSpec));
        assertTrue(templateSpecSet.contains(testPMTemplateSpec));

    }

    @Test
    public void testGetTemplateSpecsByEntityType() {
        TemplateSpec testVMTemplateSpec = TemplateSpec.newBuilder()
            .setId(1)
            .setName("testVM")
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .build();
        TemplateSpec testPMTemplateSpec = TemplateSpec.newBuilder()
            .setId(2)
            .setName("testPM")
            .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
            .build();
        Map<String, TemplateSpec> templateSpecMap = new HashMap<>();
        templateSpecMap.put(EntityType.VIRTUAL_MACHINE.toString(), testVMTemplateSpec);
        templateSpecMap.put(EntityType.PHYSICAL_MACHINE.toString(), testPMTemplateSpec);
        Mockito.when(templateSpecParser.getTemplateSpecMap()).thenReturn(templateSpecMap);
        final GetTemplateSpecByEntityTypeRequest request = GetTemplateSpecByEntityTypeRequest.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .build();
        final TemplateSpec templateSpec = templateSpecServiceBlockingStub.getTemplateSpecByEntityType(request);
        assertEquals(templateSpec, testVMTemplateSpec);
    }
}
