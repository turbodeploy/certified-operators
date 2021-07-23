package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.protobuf.TextFormat;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.skyscreamer.jsonassert.JSONAssert;

import com.vmturbo.api.component.external.api.mapper.EntityFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.TopologyDataDefinitionMapper;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.topologydefinition.TopoDataDefContextBasedApiDTO;
import com.vmturbo.api.dto.topologydefinition.TopologyDataDefinitionApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionMoles.TopologyDataDefinitionServiceMole;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.CreateTopologyDataDefinitionRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.CreateTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.DeleteTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionID;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.UpdateTopologyDataDefinitionRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.UpdateTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Testing topology definition service.
 */
public class TopologyDataDefinitionServiceTest {

    private static final String AUTOMATED_API_DTO = "/topologydefinition/TestAutomatedDefinition.json";
    private static final String MANUAL_CONTEXT_BASED_API_DTO = "/topologydefinition/TestManualContextBasedDefinition.json";
    private static final String MANUAL_API_DTO = "/topologydefinition/TestManualDefinition.json";
    private static final String AUTOMATED_PROTO = "/topologydefinition/TestAutomatedDefinition.proto";
    private static final String MANUAL_PROTO = "/topologydefinition/TestManualDefinition.proto";
    private static final String MANUAL_CONTEXT_BASED_PROTO = "/topologydefinition/TestManualContextBasedDefinition.proto";

    private TopologyDataDefinitionApiDTO manualApiDTO;
    private TopoDataDefContextBasedApiDTO manualContextBasedApiDTO;
    private TopologyDataDefinitionApiDTO automatedApiDTO;

    private TopologyDataDefinition manualProto;
    private TopologyDataDefinition automatedProto;
    private TopologyDataDefinition manualContextBasedProto;

    private TopologyDataDefinitionServiceMole spy = spy(new TopologyDataDefinitionServiceMole());

    private EntityFilterMapper filterMapper = new EntityFilterMapper(
            new GroupUseCaseParser("groupBuilderUsecases.json"),
            Mockito.mock(ThinTargetCache.class));
    private GroupsService groupsService = Mockito.mock(GroupsService.class);
    private TopologyDataDefinitionMapper mapper = new TopologyDataDefinitionMapper(filterMapper, groupsService);

    private TopologyDataDefinitionService service;
    private TopologyDataDefinitionService serviceWithEnabledContextBasedATDs;

    @Rule
    public GrpcTestServer grpcServer =
            GrpcTestServer.newServer(spy);

    @Before
    public void setUp() throws Exception {

        String manualJsonText = IOUtils.toString(
                this.getClass().getResourceAsStream(MANUAL_API_DTO),
                "UTF-8"
        );
        String manualContextBasedJsonText = IOUtils.toString(
                this.getClass().getResourceAsStream(MANUAL_CONTEXT_BASED_API_DTO),
                "UTF-8"
        );
        String automatedJsonText = IOUtils.toString(
                this.getClass().getResourceAsStream(AUTOMATED_API_DTO),
                "UTF-8"
        );
        String manualProtoText = IOUtils.toString(
                this.getClass().getResourceAsStream(MANUAL_PROTO),
                "UTF-8"
        );
        String automatedProtoText = IOUtils.toString(
                this.getClass().getResourceAsStream(AUTOMATED_PROTO),
                "UTF-8"
        );
        String manualContextBasedProtoText = IOUtils.toString(
                this.getClass().getResourceAsStream(MANUAL_CONTEXT_BASED_PROTO),
                "UTF-8"
        );
        ObjectMapper objectMapper = new ObjectMapper();
        manualApiDTO = objectMapper.readValue(manualJsonText, TopologyDataDefinitionApiDTO.class);
        manualContextBasedApiDTO =
                objectMapper.readValue(manualContextBasedJsonText, TopoDataDefContextBasedApiDTO.class);
        automatedApiDTO = objectMapper.readValue(automatedJsonText, TopologyDataDefinitionApiDTO.class);
        manualProto = TextFormat.parse(manualProtoText, TopologyDataDefinition.class);
        automatedProto = TextFormat.parse(automatedProtoText, TopologyDataDefinition.class);
        manualContextBasedProto = TextFormat.parse(manualContextBasedProtoText, TopologyDataDefinition.class);

        TopologyDataDefinitionEntry manualEntry = TopologyDataDefinitionEntry.newBuilder()
                .setId(123)
                .setDefinition(manualProto)
                .build();
        TopologyDataDefinitionEntry automatedEntry = TopologyDataDefinitionEntry.newBuilder()
                .setId(456)
                .setDefinition(automatedProto)
                .build();
        // Context-based ATD to verify that it won't be returned from service implementation
        TopologyDataDefinitionEntry manualContextBasedEntry = TopologyDataDefinitionEntry.newBuilder()
                .setId(789)
                .setDefinition(manualContextBasedProto)
                .build();

        // No ID
        TopologyDataDefinitionEntry incorrectEntry = TopologyDataDefinitionEntry.newBuilder()
                .setDefinition(automatedProto)
                .build();

        // Mock for get all method
        when(spy.getAllTopologyDataDefinitions(any())).thenReturn(Arrays.asList(GetTopologyDataDefinitionResponse
                        .newBuilder()
                        .setTopologyDataDefinition(manualEntry)
                        .build(),
                GetTopologyDataDefinitionResponse
                        .newBuilder()
                        .setTopologyDataDefinition(automatedEntry)
                        .build(),
                GetTopologyDataDefinitionResponse
                        .newBuilder()
                        .setTopologyDataDefinition(manualContextBasedEntry)
                        .build())
        );


        // Mocks for get method
        when(spy.getTopologyDataDefinition(TopologyDataDefinitionID
                .newBuilder()
                .setId(123)
                .build())
        ).thenReturn(GetTopologyDataDefinitionResponse
                        .newBuilder()
                        .setTopologyDataDefinition(manualEntry)
                        .build()
        );

        when(spy.getTopologyDataDefinition(TopologyDataDefinitionID
                .newBuilder()
                .setId(50)
                .build())
        ).thenReturn(GetTopologyDataDefinitionResponse
                .newBuilder()
                .build()
        );

        when(spy.getTopologyDataDefinition(TopologyDataDefinitionID
                .newBuilder()
                .setId(789)
                .build())
        ).thenReturn(GetTopologyDataDefinitionResponse
                .newBuilder()
                .setTopologyDataDefinition(manualContextBasedEntry)
                .build()
        );

        // Mocks for create method
        when(spy.createTopologyDataDefinition(any())).thenReturn(
                CreateTopologyDataDefinitionResponse
                        .newBuilder()
                        .setTopologyDataDefinition(incorrectEntry)
                        .build()
        );

        when(spy.createTopologyDataDefinition(CreateTopologyDataDefinitionRequest.newBuilder()
                .setTopologyDataDefinition(automatedProto).build())).thenReturn(
                CreateTopologyDataDefinitionResponse
                        .newBuilder()
                        .setTopologyDataDefinition(automatedEntry)
                        .build()
        );

        service = new TopologyDataDefinitionService(
                TopologyDataDefinitionServiceGrpc.newBlockingStub(grpcServer.getChannel()), mapper,
                false
        );

        serviceWithEnabledContextBasedATDs = new TopologyDataDefinitionService(
                TopologyDataDefinitionServiceGrpc.newBlockingStub(grpcServer.getChannel()), mapper,
                true
        );

        // Mocks for update method
        when(spy.updateTopologyDataDefinition(UpdateTopologyDataDefinitionRequest
                .newBuilder()
                .setId(123)
                .setTopologyDataDefinition(automatedProto)
                .build())
        ).thenReturn(UpdateTopologyDataDefinitionResponse
                .newBuilder()
                .setUpdatedTopologyDataDefinition(TopologyDataDefinitionEntry.newBuilder()
                        .setId(123)
                        .setDefinition(automatedProto)
                        .build())
                .build()
        );

        when(spy.updateTopologyDataDefinition(UpdateTopologyDataDefinitionRequest
                .newBuilder()
                .setId(30)
                .setTopologyDataDefinition(automatedProto)
                .build())
        ).thenReturn(UpdateTopologyDataDefinitionResponse
                .newBuilder()
                .build()
        );

        // Mocks for delete method
        when(spy.deleteTopologyDataDefinition(TopologyDataDefinitionID
                .newBuilder()
                .setId(123)
                .build())
        ).thenReturn(DeleteTopologyDataDefinitionResponse
                .newBuilder()
                .setDeleted(true)
                .build()
        );

        when(spy.deleteTopologyDataDefinition(TopologyDataDefinitionID
                .newBuilder()
                .setId(40)
                .build())
        ).thenReturn(DeleteTopologyDataDefinitionResponse
                .newBuilder()
                .setDeleted(false)
                .build()
        );

        when(spy.deleteTopologyDataDefinition(TopologyDataDefinitionID
                .newBuilder()
                .setId(20)
                .build())
        ).thenThrow(new IllegalStateException("Something went wrong"));
    }

    /**
     * For handling expected exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Get all definitions test.
     */
    @Test
    public void getAllTopologyDefinitions() throws Exception {
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setUuid("123434550");
        Mockito.when(groupsService.getGroupByUuid("123434550", false)).thenReturn(groupApiDTO);
        List<TopologyDataDefinitionApiDTO> dtos = service.getAllTopologyDefinitions();
        assertEquals(2, dtos.size());
        ObjectMapper objectMapper = new ObjectMapper();
        manualApiDTO.setUuid("123");
        automatedApiDTO.setUuid("456");
        JSONAssert.assertEquals(objectMapper.writeValueAsString(manualApiDTO), objectMapper.writeValueAsString(dtos.get(0)), false);
        JSONAssert.assertEquals(objectMapper.writeValueAsString(automatedApiDTO), objectMapper.writeValueAsString(dtos.get(1)), false);
        manualApiDTO.setUuid(null);
        automatedApiDTO.setUuid(null);
    }

    /**
     * Get all definitions test including context-based ATDs.
     */
    @Test
    public void getAllTopologyDefinitionsWithEnabledContextBased() throws Exception {
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setUuid("123434550");
        Mockito.when(groupsService.getGroupByUuid("123434550", false))
                        .thenReturn(groupApiDTO);
        List<TopologyDataDefinitionApiDTO> dtos
                        = serviceWithEnabledContextBasedATDs.getAllTopologyDefinitions();
        assertEquals(3, dtos.size());
        ObjectMapper objectMapper = new ObjectMapper();
        manualApiDTO.setUuid("123");
        automatedApiDTO.setUuid("456");
        manualContextBasedApiDTO.setUuid("789");
        JSONAssert.assertEquals(objectMapper.writeValueAsString(manualApiDTO),
                                objectMapper.writeValueAsString(dtos.get(0)), false);
        JSONAssert.assertEquals(objectMapper.writeValueAsString(automatedApiDTO),
                                objectMapper.writeValueAsString(dtos.get(1)), false);


        TopologyDataDefinitionApiDTO contextBasedDataDefDTO = dtos.get(2);

        // Need to convert TopologyDataDefinitionApiDTO into TopoDataDefContextBasedApiDTO
        // for comparison
        TopoDataDefContextBasedApiDTO contextBasedDto = new TopoDataDefContextBasedApiDTO();
        contextBasedDto.setContextBased(true);
        contextBasedDto.setUuid("789");
        contextBasedDto.setEntityDefinitionData(contextBasedDataDefDTO.getEntityDefinitionData());
        contextBasedDto.setEntityType(contextBasedDataDefDTO.getEntityType());
        contextBasedDto.setDisplayName(contextBasedDataDefDTO.getDisplayName());

        JSONAssert.assertEquals(objectMapper.writeValueAsString(manualContextBasedApiDTO),
                                objectMapper.writeValueAsString(contextBasedDto), false);

        manualApiDTO.setUuid(null);
        automatedApiDTO.setUuid(null);
        manualContextBasedApiDTO.setUuid(null);
    }

    /**
     * Get all context-based definitions test.
     */
    @Test
    public void getAllContextBasedTopologyDefinitions() throws Exception {
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setUuid("123434550");
        Mockito.when(groupsService.getGroupByUuid("123434550", false)).thenReturn(groupApiDTO);
        List<TopoDataDefContextBasedApiDTO> dtos = service.getAllContextBasedTopologyDefinitions();
        assertEquals(1, dtos.size());
        ObjectMapper objectMapper = new ObjectMapper();
        manualContextBasedApiDTO.setUuid("789");
        JSONAssert.assertEquals(objectMapper.writeValueAsString(manualContextBasedApiDTO),
                                    objectMapper.writeValueAsString(dtos.get(0)), false);
        manualContextBasedApiDTO.setUuid(null);
    }

    /**
     * Test getting topology definitions by id.
     *
     * @throws Exception if cannot find definition by id
     */
    @Test
    public void getTopologyDefinition() throws Exception {
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setUuid("123434550");
        Mockito.when(groupsService.getGroupByUuid("123434550", false)).thenReturn(groupApiDTO);
        TopologyDataDefinitionApiDTO dto = service.getTopologyDefinition("123");
        Gson g = new Gson();
        manualApiDTO.setUuid("123");
        JSONAssert.assertEquals(g.toJson(manualApiDTO), g.toJson(dto), false);
        manualApiDTO.setUuid(null);
        expectedException.expect(UnknownObjectException.class);
        service.getTopologyDefinition("50");
    }

    /**
     * Test getting context-based topology definitions by id.
     *
     * @throws Exception if cannot find definition by id
     */
    @Test
    public void getContextBasedTopologyDefinition() throws Exception {
        GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setUuid("123434550");
        Mockito.when(groupsService.getGroupByUuid("123434550", false)).thenReturn(groupApiDTO);
        TopoDataDefContextBasedApiDTO dto = service.getContextBasedTopologyDefinition("789");
        Gson g = new Gson();
        manualContextBasedApiDTO.setUuid("789");
        JSONAssert.assertEquals(g.toJson(manualContextBasedApiDTO), g.toJson(dto), false);
        manualContextBasedApiDTO.setUuid(null);
        expectedException.expect(UnknownObjectException.class);
        service.getContextBasedTopologyDefinition("50");
    }

    /**
     * Test for creating topology definition.
     *
     * @throws OperationFailedException when creation failed
     */
    @Test
    public void createTopologyDefinition() throws OperationFailedException {
        TopologyDataDefinitionApiDTO dto = service.createTopologyDefinition(automatedApiDTO);
        Gson g = new Gson();
        automatedApiDTO.setUuid("456");
        assertEquals(g.toJson(automatedApiDTO), g.toJson(dto));
        automatedApiDTO.setUuid(null);
        expectedException.expect(IllegalStateException.class);
        service.createTopologyDefinition(manualApiDTO);
    }

    /**
     * Test for updating topology definition.
     *
     * @throws UnknownObjectException if cannot find definition by id
     * @throws OperationFailedException if update failed
     */
    @Test
    public void editTopologyDefinitionTest() throws UnknownObjectException, OperationFailedException {
        TopologyDataDefinitionApiDTO dto = service.editTopologyDefinition("123", automatedApiDTO);
        Gson g = new Gson();
        automatedApiDTO.setUuid("123");
        assertEquals(g.toJson(automatedApiDTO), g.toJson(dto));
        expectedException.expect(UnknownObjectException.class);
        service.editTopologyDefinition("30", automatedApiDTO);
    }

    /**
     * Test for deleting topology definition.
     *
     * @throws UnknownObjectException if cannot find definition by id
     * @throws OperationFailedException if deletion failed
     */
    @Test
    public void deleteTopologyDefinitionTest() throws UnknownObjectException, OperationFailedException {
        service.deleteTopologyDefinition("123");
        expectedException.expect(UnknownObjectException.class);
        service.deleteTopologyDefinition("40");
    }

    /**
     * Test for deleting topology definition (operation failed case).
     *
     * @throws UnknownObjectException if cannot find definition by id
     * @throws OperationFailedException if deletion failed
     */
    @Test
    public void deleteOperationFailedTest() throws UnknownObjectException, OperationFailedException {
        expectedException.expect(OperationFailedException.class);
        service.deleteTopologyDefinition("20");
    }
}
