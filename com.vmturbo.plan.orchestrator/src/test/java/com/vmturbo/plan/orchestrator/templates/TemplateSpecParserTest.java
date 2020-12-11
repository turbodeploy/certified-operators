package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecResource;
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
        assertEquals(4, templateSpecParserMap.keySet().size());
        assertTrue(templateSpecParserMap.keySet().contains(EntityType.VIRTUAL_MACHINE.toString()));
        assertTrue(templateSpecParserMap.keySet().contains(EntityType.PHYSICAL_MACHINE.toString()));
        assertFalse(templateSpecParserMap.get(EntityType.VIRTUAL_MACHINE.toString())
            .getResourcesList().isEmpty());
        assertFalse(templateSpecParserMap.get(EntityType.VIRTUAL_MACHINE.toString())
            .getResourcesList().get(0).getFieldsList().isEmpty());
        assertEquals(templateSpecParserMap.get(EntityType.VIRTUAL_MACHINE.toString())
            .getResourcesList().get(0).getFieldsList().get(0).getName(), "numOfCpu");
    }

    /**
     * Test fields specific for HCI Host template.
     */
    @Test
    public void testHciHostTemplate() {
        TemplateSpec template = templateSpecParser.getTemplateSpecMap()
                .get(EntityType.HCI_PHYSICAL_MACHINE.toString());

        TemplateSpecResource resource = template.getResourcesList().stream()
                .filter(r -> r.getCategory().getName() == ResourcesCategoryName.Storage).findFirst()
                .get();

        testHciHostTemplateField(resource, "failuresToTolerate", null);
        testHciHostTemplateField(resource, "redundancyMethod", null);

        Assert.assertNotNull(template);
    }

    private static void testHciHostTemplateField(@Nonnull TemplateSpecResource resource,
            @Nonnull String fieldName, @Nullable Double defaultValue) {
        Optional<TemplateSpecField> field = resource.getFieldsList().stream()
                .filter(f -> f.getName().equals(fieldName)).findFirst();
        Assert.assertTrue(field.isPresent());

        if (defaultValue != null) {
            Assert.assertEquals(defaultValue.floatValue(), field.get().getDefaultValue(), .1);
        }
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
                .toJson(foomap, TemplateSpecParser.TYPE)), null
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