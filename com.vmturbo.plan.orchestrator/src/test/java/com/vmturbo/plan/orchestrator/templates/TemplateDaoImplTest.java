package com.vmturbo.plan.orchestrator.templates;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
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
        IdentityGenerator.initPrefix(0);
        prepareDatabase();
    }

    private void prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();
        final DSLContext dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        templatesDao = new TemplatesDaoImpl(dsl, "emptyDefaultTemplates.json");
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
    public void testGetTemplatesByNameEmpty() {
        assertTrue(templatesDao.getTemplatesByName("foo").isEmpty());
    }

    @Test
    public void testLoadDefaultTemplates() {
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dbConfig.dsl(), "testDefaultTemplates.json");
        final List<Template> templates = templatesDao.getTemplatesByName("testVM");
        assertThat(templates.size(), is(1));
        Template defaultTemplate = templates.get(0);
        assertThat(defaultTemplate.getType(), is(Type.SYSTEM));
        assertThat(defaultTemplate.getTemplateInfo().getName(), is("testVM"));
    }

    @Test
    public void testDeleteDefaultTemplates() {
        new TemplatesDaoImpl(dbConfig.dsl(), "testDefaultTemplates.json");
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dbConfig.dsl(), "emptyDefaultTemplates.json");
        assertTrue(templatesDao.getAllTemplates().isEmpty());
    }

    @Test
    public void testEditDefaultTemplates() {
        new TemplatesDaoImpl(dbConfig.dsl(), "testDefaultTemplates.json");
        final TemplatesDao templatesDao =
                new TemplatesDaoImpl(dbConfig.dsl(), "testModifiedDefaultTemplates.json");
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
        new TemplatesDaoImpl(dbConfig.dsl(), "testDefaultTemplates.json");
        List<Template> result = templatesDao.getTemplatesByName("testVM");
        assertEquals(1, result.size());
        assertEquals("testVM", result.get(0).getTemplateInfo().getName());
    }
}
