package com.vmturbo.group.topologydatadefinition;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition.TagBasedGenerationAndConnection;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for TopologyDataDefinitionAttributeExtractor.
 */
public class TopologyDataDefinitionAttributeExtractorTest {

    private static final String TEST_ENTITY_DEF = "TEST_ENTITY_DEFINITION_NAME";

    private static final EntityType TEST_ENTITY_TYPE = EntityType.SERVICE;

    private static final String TEST_TAG_KEY = "TAG-KEY";

    /**
     * Test that manual TopologyDataDefinition has its attributes properly extracted.
     *
     * @throws Exception if there is an identity store exception.
     */
    @Test
    public void testManualTopologyDataDefinition() throws Exception {
        final TopologyDataDefinition topologyDataDefinition = TopologyDataDefinition.newBuilder()
            .setManualEntityDefinition(ManualEntityDefinition.newBuilder()
                .setEntityType(TEST_ENTITY_TYPE)
                .setEntityName(TEST_ENTITY_DEF)
                .build())
            .build();
        TopologyDataDefinitionAttributeExtractor extractor =
            new TopologyDataDefinitionAttributeExtractor();
        IdentityMatchingAttributes attributes = extractor.extractAttributes(topologyDataDefinition);
        assertEquals(3, attributes.getMatchingAttributes().size());
        assertEquals(TEST_ENTITY_DEF, attributes.getMatchingAttribute(
            TopologyDataDefinitionAttributeExtractor.NAME_ATTRIBUTE).getAttributeValue());
        assertEquals(TEST_ENTITY_TYPE.name(), attributes.getMatchingAttribute(
            TopologyDataDefinitionAttributeExtractor.ENTITY_TYPE_ATTRIBUTE).getAttributeValue());
        assertEquals(TopologyDataDefinitionAttributeExtractor.MANUAL_DEFINITION,
            attributes.getMatchingAttribute(TopologyDataDefinitionAttributeExtractor
                .DEFINITION_TYPE_ATTRIBUTE).getAttributeValue());
    }

    /**
     * Test that automated TopologyDataDefinition has its attributes properly extracted.
     *
     * @throws Exception if there is an identity store exception.
     */
    @Test
    public void testAutomatedTopologyDataDefinition() throws Exception {
        final TopologyDataDefinition topologyDataDefinition = TopologyDataDefinition.newBuilder()
            .setAutomatedEntityDefinition(AutomatedEntityDefinition.newBuilder()
                .setEntityType(TEST_ENTITY_TYPE)
                .setNamingPrefix(TEST_ENTITY_DEF)
                .setTagGrouping(TagBasedGenerationAndConnection.newBuilder()
                    .setTagKey(TEST_TAG_KEY)
                    .build())
                .build())
            .build();
        TopologyDataDefinitionAttributeExtractor extractor =
            new TopologyDataDefinitionAttributeExtractor();
        IdentityMatchingAttributes attributes = extractor.extractAttributes(topologyDataDefinition);
        assertEquals(3, attributes.getMatchingAttributes().size());
        assertEquals(TEST_ENTITY_DEF + TEST_TAG_KEY, attributes.getMatchingAttribute(
            TopologyDataDefinitionAttributeExtractor.NAME_ATTRIBUTE).getAttributeValue());
        assertEquals(TEST_ENTITY_TYPE.name(), attributes.getMatchingAttribute(
            TopologyDataDefinitionAttributeExtractor.ENTITY_TYPE_ATTRIBUTE).getAttributeValue());
        assertEquals(TopologyDataDefinitionAttributeExtractor.AUTOMATED_DEFINITION,
            attributes.getMatchingAttribute(TopologyDataDefinitionAttributeExtractor
                .DEFINITION_TYPE_ATTRIBUTE).getAttributeValue());
    }
}
