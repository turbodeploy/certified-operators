package com.vmturbo.plan.orchestrator.templates;

import static com.vmturbo.plan.orchestrator.db.tables.Template.TEMPLATE;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.plan.TemplateDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Template;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateRecord;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Implementation of {@link TemplatesDao} using underlying DB connection.
 */
public class TemplatesDaoImpl implements TemplatesDao {

    private final DSLContext dsl;

    public TemplatesDaoImpl(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Get all existing templates.
     *
     * @return Set of all existing templates
     */
    @Override
    public Set<TemplateDTO.Template> getAllTemplates() {
        final List<Template> allTemplates = dsl.selectFrom(TEMPLATE).fetch().into(Template.class);
        return convertToProtoTemplate(allTemplates);
    }

    /**
     * Get the template by template id.
     *
     * @param id of template
     * @return Optional Template, if not found, will return Optional.empty().
     */
    @Override
    public Optional<TemplateDTO.Template> getTemplate(long id) {
        return getTemplate(dsl, id);
    }

    /**
     * Create a new template
     *
     * @param templateInfo describe the contents of one template
     * @return new created template
     */
    @Override
    public TemplateDTO.Template createTemplate(@Nonnull TemplateInfo templateInfo) {
        Template template = new Template(IdentityGenerator.next(), null,
            templateInfo.getName(), templateInfo.getEntityType(), templateInfo, null);
        dsl.newRecord(TEMPLATE, template).store();
        return TemplateDTO.Template.newBuilder()
            .setId(template.getId())
            .setTemplateInfo(template.getTemplateInfo())
            .build();
    }

    /**
     * Update a existing template with new template instance.
     *
     * @param id of existing template
     * @param templateInfo the new template instance need to store
     * @return Updated template
     * @throws NoSuchObjectException if not found existing template.
     */
    @Override
    public TemplateDTO.Template editTemplate(long id, @Nonnull TemplateInfo templateInfo) throws NoSuchObjectException {
        TemplateDTO.Template template = getTemplate(id).orElseThrow(() -> noSuchObjectException(id));

        dsl.update(TEMPLATE)
            .set(TEMPLATE.NAME, templateInfo.getName())
            .set(TEMPLATE.TEMPLATE_TYPE, templateInfo.getEntityType())
            .set(TEMPLATE.TEMPLATE_INFO, templateInfo)
            .where(TEMPLATE.ID.eq(id))
            .execute();

        TemplateDTO.Template.Builder builder = TemplateDTO.Template.newBuilder()
            .setId(template.getId())
            .setTemplateInfo(templateInfo);
        if (template.hasTargetId()) {
            builder.setTargetId(template.getTargetId());
        }
        return builder.build();
    }

    /**
     * Delete a existing template.
     *
     * @param id of existing template
     * @return Deleted template.
     * @throws NoSuchObjectException if not find existing template.
     */
    @Override
    public TemplateDTO.Template deleteTemplateById(long id) throws NoSuchObjectException {
        TemplateDTO.Template template = getTemplate(id).orElseThrow(() -> noSuchObjectException(id));
        dsl.deleteFrom(TEMPLATE).where(TEMPLATE.ID.eq(id)).execute();
        return template;
    }

    /**
     * Delete discovered templates by target id.
     *
     * @param targetId id of target
     * @return all deleted Template object
     */
    @Override
    public List<TemplateDTO.Template> deleteTemplateByTargetId(long targetId) {
        return dsl.transactionResult(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            List<TemplateDTO.Template> deletedRecords = transactionDsl
                .selectFrom(TEMPLATE).where(TEMPLATE.TARGET_ID.eq(targetId)).fetch().into(Template.class)
                .stream()
                .map(template -> {
                    TemplateDTO.Template.Builder builder = TemplateDTO.Template.newBuilder()
                        .setId(template.getId())
                        .setTemplateInfo(template.getTemplateInfo());
                    if (template.getTargetId() != null) {
                        builder.setTargetId(template.getTargetId());
                    }
                    return builder.build();
                }).collect(Collectors.toList());
            transactionDsl.deleteFrom(TEMPLATE).where(TEMPLATE.TARGET_ID.eq(targetId)).execute();
            return deletedRecords;
        });
    }

    /**
     * Get all templates by query entity type
     *
     * @param type entity type of template
     * @return all selected Template object
     */
    @Override
    public Set<TemplateDTO.Template> getTemplatesByType(int type) {
        final List<Template> templates = dsl.selectFrom(TEMPLATE)
            .where(TEMPLATE.TEMPLATE_TYPE.eq(type))
            .fetch()
            .into(Template.class);

        return convertToProtoTemplate(templates);
    }

    /**
     * Get a set of templates by template id list.
     *
     * @param ids Set of template ids.
     * @return  Set of templates.
     */
    @Override
    public Set<TemplateDTO.Template> getTemplates(@Nonnull Set<Long> ids) {
        final List<Template> templateList =
            dsl.selectFrom(TEMPLATE).where(TEMPLATE.ID.in(ids)).fetch().into(Template.class);
        return convertToProtoTemplate(templateList);
    }

    private static Optional<TemplateDTO.Template> getTemplate(@Nonnull final DSLContext dsl,
                                                              final long id) {
        final TemplateRecord templateInstance =
            dsl.selectFrom(TEMPLATE).where(TEMPLATE.ID.eq(id)).fetchOne();

        return Optional.ofNullable(templateInstance)
            .map(templateRecord -> templateRecord.into(Template.class))
            .map(template -> {
                TemplateDTO.Template.Builder builder = TemplateDTO.Template.newBuilder()
                    .setId(template.getId())
                    .setTemplateInfo(template.getTemplateInfo());
                if (template.getTargetId() != null) {
                    builder.setTargetId(template.getTargetId());
                }
                return builder.build();
            });
    }

    private static NoSuchObjectException noSuchObjectException(long id) {
        return new NoSuchObjectException("Template with id " + id + " not found");
    }

    private Set<TemplateDTO.Template> convertToProtoTemplate(@Nonnull List<Template> pojTemplates) {
        return pojTemplates.stream()
            .map(template -> {
                TemplateDTO.Template.Builder builder = TemplateDTO.Template.newBuilder()
                    .setId(template.getId())
                    .setTemplateInfo(template.getTemplateInfo());
                if (template.getTargetId() != null) {
                    builder.setTargetId(template.getTargetId());
                }
                return builder.build();
            })
            .collect(Collectors.toSet());
    }
}
