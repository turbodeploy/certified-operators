package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
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
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        templateSpecParser.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diags.capture());

        assertEquals(Collections.singletonList(
                TemplateSpecParser.GSON.toJson(templateSpecParser.getTemplateSpecMap(),
                        TemplateSpecParser.TYPE)), diags.getAllValues());
    }

    @Test
    public void testRestoreDiags() throws Exception {
        final TemplateSpec foo = TemplateSpec.newBuilder().setName("FOO").build();
        final Map<String, TemplateSpec> foomap = ImmutableMap.of("foo", foo);

        try {
            templateSpecParser.restoreDiags(Collections.singletonList(TemplateSpecParser.GSON
                .toJson(foomap, TemplateSpecParser.TYPE))
            );
            fail();
        } catch (DiagnosticsException e) {
            assertTrue(e.hasErrors());
            assertEquals(1, e.getErrors().size());
            assertTrue(e.getErrors().get(0).contains("preexisting template specs"));
        }

        assertEquals(1, templateSpecParser.getTemplateSpecMap().size());
        assertEquals(foo, templateSpecParser.getTemplateSpecMap().get("foo"));
    }
}