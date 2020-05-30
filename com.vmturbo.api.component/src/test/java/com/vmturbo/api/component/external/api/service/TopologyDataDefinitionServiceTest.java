package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;
import com.google.protobuf.TextFormat;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.component.external.api.mapper.TopologyDataDefinitionMapper;
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

/**
 * Testing topology definition service.
 */
public class TopologyDataDefinitionServiceTest {

    private static final String AUTOMATED_API_DTO = "/topologydefinition/TestAutomatedDefinition.json";
    private static final String MANUAL_API_DTO = "/topologydefinition/TestManualDefinition.json";
    private static final String AUTOMATED_PROTO = "/topologydefinition/TestAutomatedDefinition.proto";
    private static final String MANUAL_PROTO = "/topologydefinition/TestManualDefinition.proto";

    private TopologyDataDefinitionApiDTO manualApiDTO;
    private TopologyDataDefinitionApiDTO automatedApiDTO;

    private TopologyDataDefinition manualProto;
    private TopologyDataDefinition automatedProto;

    private TopologyDataDefinitionServiceMole spy = spy(new TopologyDataDefinitionServiceMole());

    private TopologyDataDefinitionMapper mapper = new TopologyDataDefinitionMapper();

    private TopologyDataDefinitionService service;

    @Rule
    public GrpcTestServer grpcServer =
            GrpcTestServer.newServer(spy);

    @Before
    public void setUp() throws Exception {

        String manualJsonText = IOUtils.toString(
                this.getClass().getResourceAsStream(MANUAL_API_DTO),
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
        Gson g = new Gson();
        manualApiDTO = g.fromJson(manualJsonText, TopologyDataDefinitionApiDTO.class);
        automatedApiDTO = g.fromJson(automatedJsonText, TopologyDataDefinitionApiDTO.class);
        manualProto = TextFormat.parse(manualProtoText, TopologyDataDefinition.class);
        automatedProto = TextFormat.parse(automatedProtoText, TopologyDataDefinition.class);

        TopologyDataDefinitionEntry manualEntry = TopologyDataDefinitionEntry.newBuilder()
                .setId(123)
                .setDefinition(manualProto)
                .build();
        TopologyDataDefinitionEntry automatedEntry = TopologyDataDefinitionEntry.newBuilder()
                .setId(456)
                .setDefinition(automatedProto)
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
                TopologyDataDefinitionServiceGrpc.newBlockingStub(grpcServer.getChannel()), mapper
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
    public void getAllTopologyDefinitions() {
        List<TopologyDataDefinitionApiDTO> dtos = service.getAllTopologyDefinitions();
        assertEquals(2, dtos.size());
        Gson g = new Gson();
        manualApiDTO.setUuid("123");
        automatedApiDTO.setUuid("456");
        assertEquals(g.toJson(manualApiDTO), g.toJson(dtos.get(0)));
        assertEquals(g.toJson(automatedApiDTO), g.toJson(dtos.get(1)));
        manualApiDTO.setUuid(null);
        automatedApiDTO.setUuid(null);
    }

    /**
     * Test getting topology definitions by id.
     *
     * @throws UnknownObjectException if cannot find definition by id
     */
    @Test
    public void getTopologyDefinition() throws UnknownObjectException {
        TopologyDataDefinitionApiDTO dto = service.getTopologyDefinition("123");
        Gson g = new Gson();
        manualApiDTO.setUuid("123");
        assertEquals(g.toJson(manualApiDTO), g.toJson(dto));
        manualApiDTO.setUuid(null);
        expectedException.expect(UnknownObjectException.class);
        service.getTopologyDefinition("50");
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