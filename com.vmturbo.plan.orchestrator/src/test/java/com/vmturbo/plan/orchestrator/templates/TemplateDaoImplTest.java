package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
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
        templatesDao = new TemplatesDaoImpl(dsl);
        flyway.clean();
        flyway.migrate();
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

        assertEquals(retrievedTemplate.get(), createdTemplate);
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
    public void testEditTemplate() throws NoSuchObjectException {
        TemplateInfo firstTemplateInstance = TemplateInfo.newBuilder()
            .setName("first-template-instance")
            .build();
        TemplateInfo secondTemplateInstance = TemplateInfo.newBuilder()
            .setName("second-template-instance")
            .build();
        Template createdFirstTemplate = templatesDao.createTemplate(firstTemplateInstance);
        Template newTemplate = templatesDao.editTemplate(createdFirstTemplate.getId(), secondTemplateInstance);
        Optional<Template> retrievedTemplate = templatesDao.getTemplate(createdFirstTemplate.getId());
        assertEquals(retrievedTemplate.get(), newTemplate);
    }

    @Test
    public void testDeleteTemplate() throws NoSuchObjectException {
        TemplateInfo templateInstance = TemplateInfo.newBuilder()
            .setName("template-instance")
            .build();
        Template createdTemplate = templatesDao.createTemplate(templateInstance);
        Template deletedTemplate = templatesDao.deleteTemplateById(createdTemplate.getId());
        assertEquals(createdTemplate, deletedTemplate);
    }
}
