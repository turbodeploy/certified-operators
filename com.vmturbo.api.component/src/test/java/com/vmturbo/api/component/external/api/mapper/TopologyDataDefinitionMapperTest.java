package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.gson.Gson;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.TextFormat;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.api.dto.topologydefinition.TopologyDataDefinitionApiDTO;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition.AssociatedEntitySelectionCriteria;

/**
 * Tests for {@link TopologyDataDefinitionMapper}.
 */
public class TopologyDataDefinitionMapperTest {

    private static final String AUTOMATED_API_DTO = "/topologydefinition/TestAutomatedDefinition.json";
    private static final String MANUAL_API_DTO = "/topologydefinition/TestManualDefinition.json";
    private static final String AUTOMATED_PROTO = "/topologydefinition/TestAutomatedDefinition.proto";
    private static final String MANUAL_PROTO = "/topologydefinition/TestManualDefinition.proto";

    private static final String INCORRECT_AUTOMATED_API_DTO = "/topologydefinition/IncorrectAutomatedDefinition.json";
    private static final String INCORRECT_MANUAL_API_DTO = "/topologydefinition/IncorrectManualDefinition.json";
    private static final String INCORRECT_AUTOMATED_PROTO = "/topologydefinition/IncorrectAutomatedDefinition.proto";
    private static final String INCORRECT_MANUAL_PROTO = "/topologydefinition/IncorrectManualDefinition.proto";

    private TopologyDataDefinitionApiDTO manualApiDTO;
    private TopologyDataDefinitionApiDTO automatedApiDTO;

    private TopologyDataDefinition manualProto;
    private TopologyDataDefinition automatedProto;

    private TopologyDataDefinitionMapper mapper = new TopologyDataDefinitionMapper();

    /**
     * Setting up.
     *
     * @throws IOException if file processing fails
     */
    @Before
    public void setUp() throws IOException {
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
    }

    /**
     * Test for converting XL definition into API DTO.
     */
    @Test
    public void convertTopologyDataDefinitionApiDTOTest() {
        TopologyDataDefinition manualDefinition = mapper.convertTopologyDataDefinitionApiDTO(manualApiDTO);
        TopologyDataDefinition automatedDefinition = mapper.convertTopologyDataDefinitionApiDTO(automatedApiDTO);
        assertEquals(automatedProto.getAllFields(), automatedDefinition.getAllFields());
        ManualEntityDefinition expectedManualDef = manualProto.getManualEntityDefinition();
        ManualEntityDefinition actualManualDef = manualDefinition.getManualEntityDefinition();
        assertEquals(expectedManualDef.getEntityType(), actualManualDef.getEntityType());
        assertEquals(expectedManualDef.getEntityName(), actualManualDef.getEntityName());
        List<AssociatedEntitySelectionCriteria> expectedList
                = new ArrayList<>(expectedManualDef.getAssociatedEntitiesList());
        List<AssociatedEntitySelectionCriteria> actualList
                = new ArrayList<>(actualManualDef.getAssociatedEntitiesList());
        expectedList.sort(Comparator.comparing(AbstractMessage::toString));
        actualList.sort(Comparator.comparing(AbstractMessage::toString));
        assertEquals(expectedList, actualList);
    }

    /**
     * Test for converting API DTO into XL definition.
     */
    @Test
    public void convertTopologyDataDefinitionTest() {
        TopologyDataDefinitionApiDTO manualDefinitionApiDTO = mapper.convertTopologyDataDefinition(manualProto);
        TopologyDataDefinitionApiDTO automatedDefinitionApiDTO = mapper.convertTopologyDataDefinition(automatedProto);
        assertEquals(automatedApiDTO.toString(), automatedDefinitionApiDTO.toString());
        assertEquals(manualApiDTO.toString(), manualDefinitionApiDTO.toString());
    }

    /**
     * For handling expected exceptions.
     */
    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Incorrect automated api dto (expected field not specified).
     *
     * @throws IOException if cannot process file
     */
    @Test
    public void incorrectAutomatedApiDtoTest() throws IOException {
        String incorrectText = IOUtils.toString(
                this.getClass().getResourceAsStream(INCORRECT_AUTOMATED_API_DTO),
                "UTF-8"
        );
        Gson g = new Gson();
        TopologyDataDefinitionApiDTO incorrect = g.fromJson(incorrectText, TopologyDataDefinitionApiDTO.class);
        exception.expect(IllegalArgumentException.class);
        mapper.convertTopologyDataDefinitionApiDTO(incorrect);
    }

    /**
     * Incorrect manual api dto (incorrect filter).
     *
     * @throws IOException if cannot process file
     */
    @Test
    public void incorrectManualApiDtoTest() throws IOException {
        String incorrectText = IOUtils.toString(
                this.getClass().getResourceAsStream(INCORRECT_MANUAL_API_DTO),
                "UTF-8"
        );
        Gson g = new Gson();
        TopologyDataDefinitionApiDTO incorrect = g.fromJson(incorrectText, TopologyDataDefinitionApiDTO.class);
        exception.expect(IllegalArgumentException.class);
        mapper.convertTopologyDataDefinitionApiDTO(incorrect);
    }

    /**
     * Incorrect automated proto (unsupported entity type).
     *
     * @throws IOException if cannot process file
     */
    @Test
    public void incorrectAutomatedProtoTest() throws IOException {
        String incorrectText = IOUtils.toString(
                this.getClass().getResourceAsStream(INCORRECT_AUTOMATED_PROTO),
                "UTF-8"
        );
        TopologyDataDefinition incorrect = TextFormat.parse(incorrectText, TopologyDataDefinition.class);
        exception.expect(IllegalArgumentException.class);
        mapper.convertTopologyDataDefinition(incorrect);
    }

    /**
     * Incorrect manual proto (unsupported connected entity type).
     *
     * @throws IOException if cannot process file
     */
    @Test
    public void incorrectManualProtoTest() throws IOException {
        String incorrectText = IOUtils.toString(
                this.getClass().getResourceAsStream(INCORRECT_MANUAL_PROTO),
                "UTF-8"
        );
        TopologyDataDefinition incorrect = TextFormat.parse(incorrectText, TopologyDataDefinition.class);
        exception.expect(IllegalArgumentException.class);
        mapper.convertTopologyDataDefinition(incorrect);
    }
}