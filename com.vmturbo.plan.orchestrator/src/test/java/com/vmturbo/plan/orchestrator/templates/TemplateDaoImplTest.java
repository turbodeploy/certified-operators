package com.vmturbo.plan.orchestrator.templates;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.plan.TemplateDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class TemplateDaoImplTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private TemplatesDaoImpl templatesDao;

    @Before
    public void setup() throws Exception {
        prepareDatabase();
    }

    private void prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();
        final DSLContext dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        templatesDao = new TemplatesDaoImpl(dsl, "emptyDefaultTemplates.json",
                new IdentityInitializer(0));
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testCreateTemplate() {
        TemplateInfo templateInfo = TemplateInfo.newBuilder()
            .setName("template-instance")
            .build();
        Template result = templatesDao.createTemplate(templateInfo);
        assertEquals(result.getTemplateInfo(), templateInfo);
    }

    @Test
    public void testGetTemplate() {
        TemplateInfo templateInfo = TemplateInfo.newBuilder()
            .setName("template-instance")
            .build();
        Template createdTemplate = templatesDao.createTemplate(templateInfo);
        Optional<Template> retrievedTemplate = templatesDao.getTemplate(createdTemplate.getId());

        assertThat(retrievedTemplate.get(), is(createdTemplate));
    }

    @Test
    public void testGetAllTemplate() {
        TemplateInfo firstTemplateInstance = TemplateInfo.newBuilder()
            .setName("first-template-instance")
            .build();
        TemplateInfo secondTemplateInstance = TemplateInfo.newBuilder()
            .setName("second-template-instance")
            .build();
        Template createdFirstTemplate = templatesDao.createTemplate(firstTemplateInstance);
        Template createdSecondTemplate = templatesDao.createTemplate(secondTemplateInstance);
        Set<Template> retrievedTemplates = templatesDao.getAllTemplates();
        assertTrue(retrievedTemplates.size() == 2);
        assertTrue(retrievedTemplates.stream()
            .anyMatch(template -> template.getId() == createdFirstTemplate.getId()));
        assertTrue(retrievedTemplates.stream()
            .anyMatch(template -> template.getId() == createdSecondTemplate.getId()));
    }

    @Test
    public void testEditTemplate()
            throws NoSuchObjectException, IllegalTemplateOperationException {
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
    public void testDeleteTemplate()
            throws NoSuchObjectException, IllegalTemplateOperationException {
        TemplateInfo templateInstance = TemplateInfo.newBuilder()
            .setName("template-instance")
            .build();
        Template createdTemplate = templatesDao.createTemplate(templateInstance);
        Template deletedTemplate = templatesDao.deleteTemplateById(createdTemplate.getId());
        assertThat(deletedTemplate, is(createdTemplate));
    }

    @Test
    public void testGetTemplatesByName() {
        final TemplateInfo templateInstance = TemplateInfo.newBuilder()
                .setName("bar")
                .build();
        final Template createdTemplate = templatesDao.createTemplate(templateInstance);
        final List<Template> byName = templatesDao.getTemplatesByName("bar");
        assertThat(byName, containsInAnyOrder(createdTemplate));
    }

    @Test
    public void testGetTemplatesCountByIds() {
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
        assertTrue(templatesDao.getTemplatesByName("foo").isEmpty());
    }

    @Test
    public void testLoadDefaultTemplates() {
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dbConfig.dsl(), "testDefaultTemplates.json",
                        new IdentityInitializer(0));
        final List<Template> templates = templatesDao.getTemplatesByName("testVM");
        assertThat(templates.size(), is(1));
        Template defaultTemplate = templates.get(0);
        assertThat(defaultTemplate.getType(), is(Type.SYSTEM));
        assertThat(defaultTemplate.getTemplateInfo().getName(), is("testVM"));
    }

    @Test
    public void testDeleteDefaultTemplates() {
        new TemplatesDaoImpl(dbConfig.dsl(), "testDefaultTemplates.json",
                new IdentityInitializer(0));
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dbConfig.dsl(), "emptyDefaultTemplates.json",
                        new IdentityInitializer(0));
        assertTrue(templatesDao.getAllTemplates().isEmpty());
    }

    @Test
    public void testEditDefaultTemplates() {
        new TemplatesDaoImpl(dbConfig.dsl(), "testDefaultTemplates.json",
                new IdentityInitializer(0));
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dbConfig.dsl(), "testModifiedDefaultTemplates.json",
                        new IdentityInitializer(0));
        final List<Template> templates = templatesDao.getTemplatesByName("testVM");
        assertThat(templates.size(), is(1));
        Template defaultTemplate = templates.get(0);
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
        new TemplatesDaoImpl(dbConfig.dsl(), "testDefaultTemplates.json",
                new IdentityInitializer(0));
        List<Template> result = templatesDao.getTemplatesByName("testVM");
        assertEquals(1, result.size());
        assertEquals("testVM", result.get(0).getTemplateInfo().getName());
    }

    @Test
    public void testCollectDiags() throws Exception {
        final Template foo =
            templatesDao.createTemplate(TemplateInfo.newBuilder().setName("foo").build());
        final Template bar =
            templatesDao.createTemplate(TemplateInfo.newBuilder().setName("bar").build());
        final List<Template> expected = Arrays.asList(foo, bar);


        final List<String> diags = templatesDao.collectDiags();
        System.out.println(diags);
        assertEquals(2, diags.size());

        assertTrue(diags.stream()
            .map(string -> TemplatesDaoImpl.GSON.fromJson(string, Template.class))
            .allMatch(expected::contains));

    }

    @Test
    public void testRestoreDiags() throws Exception {
        final List<String> serialized = Arrays.asList(
            "{\"id\":\"1997522616832\",\"templateInfo\":{\"name\":\"bar\"},\"type\":\"USER\"}",
            "{\"id\":\"1997522614816\",\"templateInfo\":{\"name\":\"foo\"},\"type\":\"USER\"}"
        );

        templatesDao.restoreDiags(serialized);

        assertTrue(templatesDao.getAllTemplates().stream()
            .map(template -> TemplatesDaoImpl.GSON.toJson(template, Template.class))
            .allMatch(serialized::contains));
    }
}
