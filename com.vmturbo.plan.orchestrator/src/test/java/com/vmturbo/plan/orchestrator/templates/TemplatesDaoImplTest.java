package com.vmturbo.plan.orchestrator.templates;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit tests for {@link TemplatesDao}.
 */
public class TemplatesDaoImplTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Plan.PLAN);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private DSLContext dsl = dbConfig.getDslContext();

    private TemplatesDaoImpl templatesDao = new TemplatesDaoImpl(dsl, "emptyDefaultTemplates.json",
        new IdentityInitializer(0));

    /**
     * Captures expected exceptions in a test.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreateTemplate() throws DuplicateTemplateException {
        TemplateInfo templateInfo = TemplateInfo.newBuilder()
            .setName("template-instance")
            .build();
        Template result = templatesDao.createTemplate(templateInfo);
        assertEquals(result.getTemplateInfo(), templateInfo);
    }

    @Test
    public void testGetTemplate() throws DuplicateTemplateException {
        TemplateInfo templateInfo = TemplateInfo.newBuilder()
            .setName("template-instance")
            .build();
        Template createdTemplate = templatesDao.createTemplate(templateInfo);
        Optional<Template> retrievedTemplate = templatesDao.getTemplate(createdTemplate.getId());

        assertThat(retrievedTemplate.get(), is(createdTemplate));
    }

    @Test
    public void testGetAllTemplate() throws DuplicateTemplateException {
        TemplateInfo firstTemplateInstance = TemplateInfo.newBuilder()
            .setName("first-template-instance")
            .build();
        TemplateInfo secondTemplateInstance = TemplateInfo.newBuilder()
            .setName("second-template-instance")
            .build();
        Template createdFirstTemplate = templatesDao.createTemplate(firstTemplateInstance);
        Template createdSecondTemplate = templatesDao.createTemplate(secondTemplateInstance);
        Set<Template> retrievedTemplates = templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());
        assertTrue(retrievedTemplates.size() == 2);
        assertTrue(retrievedTemplates.stream()
            .anyMatch(template -> template.getId() == createdFirstTemplate.getId()));
        assertTrue(retrievedTemplates.stream()
            .anyMatch(template -> template.getId() == createdSecondTemplate.getId()));
    }

    @Test
    public void testEditTemplate()
            throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {
        TemplateInfo firstTemplateInstance = TemplateInfo.newBuilder()
            .setName("first-template-instance")
            .build();
        TemplateInfo secondTemplateInstance = TemplateInfo.newBuilder()
            .setName("second-template-instance")
            .build();
        Template createdFirstTemplate = templatesDao.createTemplate(firstTemplateInstance);
        Template newTemplate = templatesDao.editTemplate(createdFirstTemplate.getId(), secondTemplateInstance);
        Optional<Template> retrievedTemplate = templatesDao.getTemplate(createdFirstTemplate.getId());
        assertThat(newTemplate, is(retrievedTemplate.get()));
    }

    @Test
    public void testEditTemplateToDuplicateName()
            throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {
        TemplateInfo firstTemplateInstance = TemplateInfo.newBuilder()
                .setName("first-template-instance")
                .build();
        TemplateInfo duplicateTemplateInstance = TemplateInfo.newBuilder()
                .setName("second-template-instance")
                .build();

        Template createdFirstTemplate = templatesDao.createTemplate(firstTemplateInstance);
        Template duplicateTemplate = templatesDao.createTemplate(duplicateTemplateInstance);
        TemplateInfo template = duplicateTemplate.toBuilder().setId(duplicateTemplate.getId()).build().getTemplateInfo();
        expectedException.expect(DuplicateTemplateException.class);
        templatesDao.editTemplate(createdFirstTemplate.getId(), template);
    }

    @Test
    public void testDeleteTemplate()
            throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {
        TemplateInfo templateInstance = TemplateInfo.newBuilder()
            .setName("template-instance")
            .build();
        Template createdTemplate = templatesDao.createTemplate(templateInstance);
        Template deletedTemplate = templatesDao.deleteTemplateById(createdTemplate.getId());
        assertThat(deletedTemplate, is(createdTemplate));
    }

    @Test
    public void testGetTemplatesByName() throws DuplicateTemplateException {
        final TemplateInfo templateInstance = TemplateInfo.newBuilder()
                .setName("bar")
                .build();
        final Template createdTemplate = templatesDao.createTemplate(templateInstance);
        final Set<Template> byName = templatesDao.getFilteredTemplates(TemplatesFilter.newBuilder()
            .addTemplateName("bar")
            .build());
        assertThat(byName, containsInAnyOrder(createdTemplate));
    }

    @Test
    public void testGetTemplatesCountByIds() throws DuplicateTemplateException {
        final TemplateInfo barTemplate = TemplateInfo.newBuilder()
                .setName("bar")
                .build();
        final TemplateInfo fooTemplate = TemplateInfo.newBuilder()
                .setName("foo")
                .build();
        final TemplateInfo bazTemplate = TemplateInfo.newBuilder()
                .setName("baz")
                .build();
        final Template createdBarTemplate = templatesDao.createTemplate(barTemplate);
        final Template createFooTemplate = templatesDao.createTemplate(fooTemplate);
        final Template createBazTemplate = templatesDao.createTemplate(bazTemplate);
        final long count = templatesDao.getTemplatesCount(
                ImmutableSet.of(createdBarTemplate.getId(), createFooTemplate.getId()));
        assertEquals(2, count);
    }

    @Test
    public void testGetTemplatesByNameEmpty() {
        assertTrue(templatesDao.getFilteredTemplates(TemplatesFilter.newBuilder()
            .addTemplateName("foo")
            .build()).isEmpty());
    }

    @Test
    public void testLoadDefaultTemplates() {
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dsl, "testDefaultTemplates.json",
                        new IdentityInitializer(0));
        final Set<Template> templates = templatesDao.getFilteredTemplates(TemplatesFilter.newBuilder()
            .addTemplateName("testVM")
            .build());
        assertThat(templates.size(), is(1));
        Template defaultTemplate = templates.iterator().next();
        assertThat(defaultTemplate.getType(), is(Type.SYSTEM));
        assertThat(defaultTemplate.getTemplateInfo().getName(), is("testVM"));
    }

    @Test
    public void testDeleteDefaultTemplates() {
        new TemplatesDaoImpl(dsl, "testDefaultTemplates.json",
                new IdentityInitializer(0));
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dsl, "emptyDefaultTemplates.json",
                        new IdentityInitializer(0));
        assertTrue(templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance()).isEmpty());
    }

    @Test
    public void testEditDefaultTemplates() {
        new TemplatesDaoImpl(dsl, "testDefaultTemplates.json",
                new IdentityInitializer(0));
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dsl, "testModifiedDefaultTemplates.json",
                        new IdentityInitializer(0));
        final Set<Template> templates = templatesDao.getFilteredTemplates(TemplatesFilter.newBuilder()
            .addTemplateName("testVM")
            .build());
        assertThat(templates.size(), is(1));
        Template defaultTemplate = templates.iterator().next();
        final String memSizeValue = defaultTemplate.getTemplateInfo().getResourcesList().stream()
                .filter(resource -> resource.getCategory().getName().equals(ResourcesCategoryName.Compute))
                .map(resource -> resource.getFieldsList().stream()
                        .filter(field -> field.getName().equals("memorySize"))
                        .map(TemplateField::getValue)
                        .findFirst()
                        .get())
                .findFirst().get();
        assertThat(memSizeValue, is("1024.0"));
    }

    @Test
    public void testGetTemplateByName() throws Exception {
        new TemplatesDaoImpl(dsl, "testDefaultTemplates.json",
                new IdentityInitializer(0));
        Set<Template> result = templatesDao.getFilteredTemplates(TemplatesFilter.newBuilder()
            .addTemplateName("testVM")
            .build());
        assertEquals(1, result.size());
        assertEquals("testVM", result.iterator().next().getTemplateInfo().getName());
    }

    @Test
    public void testCollectDiags() throws Exception {
        final Template foo =
            templatesDao.createTemplate(TemplateInfo.newBuilder().setName("foo").build());
        final Template bar =
            templatesDao.createTemplate(TemplateInfo.newBuilder().setName("bar").build());
        final List<Template> expected = Arrays.asList(foo, bar);


        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        templatesDao.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diags.capture());

        System.out.println(diags.getAllValues());
        assertEquals(2, diags.getAllValues().size());

        assertTrue(diags.getAllValues().stream()
            .map(string -> TemplatesDaoImpl.GSON.fromJson(string, Template.class))
            .allMatch(expected::contains));

    }

    @Test
    public void testRestoreDiags() throws Exception {

        final Template preexisting =
            templatesDao.createTemplate(TemplateInfo.newBuilder().setName("preexisting").build());

        final List<String> serialized = Arrays.asList(
            "{\"id\":\"1997522616832\",\"templateInfo\":{\"name\":\"bar\"},\"type\":\"USER\"}",
            "{\"id\":\"1997522614816\",\"templateInfo\":{\"name\":\"foo\"},\"type\":\"USER\"}"
        );

        try {
            templatesDao.restoreDiags(serialized, null);
            fail();
        } catch (DiagnosticsException e) {
            assertTrue(e.hasErrors());
            assertEquals(1, e.getErrors().size());
            assertTrue(e.getErrors().get(0).contains("preexisting templates"));
        }

        assertFalse(templatesDao.getTemplate(preexisting.getId()).isPresent());
        assertTrue(templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance()).stream()
            .map(template -> TemplatesDaoImpl.GSON.toJson(template, Template.class))
            .allMatch(serialized::contains));
    }

    /**
     * Test {@link TemplatesDaoImpl#createTemplate(TemplateInfo)}.
     *
     * @throws DuplicateTemplateException if template name already exist
     */
    @Test
    public void testDuplicateTemplateNames() throws DuplicateTemplateException {
        // Create a new template.
        TemplateInfo newTemplateInfo = TemplateInfo.newBuilder()
                .setName("template-instance")
                .setDescription("a new template")
                .build();
        assertEquals(templatesDao.createTemplate(newTemplateInfo).getTemplateInfo(), newTemplateInfo);

        // Creating a template with the same name should replace the existing one.
        TemplateInfo templateInfo = TemplateInfo.newBuilder()
            .setName("template-instance")
            .setDescription("a template with the same name")
            .build();
        expectedException.expect(DuplicateTemplateException.class);
        templatesDao.createTemplate(templateInfo);
    }

    /**
     * Tests saving and retrieving of a headroom template.
     * @throws Exception when something goes wrong.
     */
    @Test
    public void setAndGetHeadroomTemplateForAGroup() throws Exception {
        TemplateInfo templateInfo1 = TemplateInfo.newBuilder()
            .setName("template-instance1")
            .build();
        Template template1 = templatesDao.createTemplate(templateInfo1);

        final long groupId = 5L;

        templatesDao.setOrUpdateHeadroomTemplateForCluster(groupId, template1.getId());

        Optional<Template> retrievedTemplate =
            templatesDao.getClusterHeadroomTemplateForGroup(groupId);

        assertTrue(retrievedTemplate.isPresent());
        assertEquals(template1, retrievedTemplate.get());

        TemplateInfo templateInfo2 = TemplateInfo.newBuilder()
            .setName("template-instance2")
            .build();
        Template template2 = templatesDao.createTemplate(templateInfo2);

        templatesDao.setOrUpdateHeadroomTemplateForCluster(groupId, template2.getId());

        retrievedTemplate =
            templatesDao.getClusterHeadroomTemplateForGroup(groupId);

        assertTrue(retrievedTemplate.isPresent());
        assertEquals(template2, retrievedTemplate.get());
    }
}
