package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

public class TemplateSpecParserTest {
    private TemplateSpecParser templateSpecParser;
    private final String templateSpecFileName = "defaultTemplateSpec.json";

    @Before
    public void init() {
        templateSpecParser = new TemplateSpecParser(templateSpecFileName);
    }

    @Test
    public void testTemplateSpecIsValid() {
        Map<String, TemplateSpec> templateSpecParserMap = templateSpecParser.getTemplateSpecMap();

        assertNotNull(templateSpecParserMap);
        assertEquals(templateSpecParserMap.keySet().size(), 3);
        assertTrue(templateSpecParserMap.keySet().contains(EntityType.VIRTUAL_MACHINE.toString()));
        assertTrue(templateSpecParserMap.keySet().contains(EntityType.PHYSICAL_MACHINE.toString()));
        assertFalse(templateSpecParserMap.get(EntityType.VIRTUAL_MACHINE.toString())
            .getResourcesList().isEmpty());
        assertFalse(templateSpecParserMap.get(EntityType.VIRTUAL_MACHINE.toString())
            .getResourcesList().get(0).getFieldsList().isEmpty());
        assertEquals(templateSpecParserMap.get(EntityType.VIRTUAL_MACHINE.toString())
            .getResourcesList().get(0).getFieldsList().get(0).getName(), "numOfCpu");
    }

    @Test
    public void testCollectDiags() throws Exception {
        assertEquals(
            Collections.singletonList(TemplateSpecParser.GSON
                .toJson(templateSpecParser.getTemplateSpecMap(), TemplateSpecParser.TYPE)),
            templateSpecParser.collectDiags()
        );
    }

    @Test
    public void testRestoreDiags() throws Exception {
        final TemplateSpec foo = TemplateSpec.newBuilder().setName("FOO").build();
        final Map<String, TemplateSpec> foomap = ImmutableMap.of("foo", foo);

        templateSpecParser.restoreDiags(Collections.singletonList(TemplateSpecParser.GSON
            .toJson(foomap, TemplateSpecParser.TYPE))
        );

        assertEquals(1, templateSpecParser.getTemplateSpecMap().size());
        assertEquals(foo, templateSpecParser.getTemplateSpecMap().get("foo"));
    }
}