package com.vmturbo.plan.orchestrator.templates;

import static com.vmturbo.plan.orchestrator.db.tables.Template.TEMPLATE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;

import com.vmturbo.common.protobuf.plan.TemplateDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Template;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateRecord;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Implementation of {@link TemplatesDao} using underlying DB connection.
 */
public class TemplatesDaoImpl implements TemplatesDao {
    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    public TemplatesDaoImpl(@Nonnull final DSLContext dsl,
                            @Nonnull final String defaultTemplatesFile,
                            @Nonnull final IdentityInitializer identityInitializer) {
        this.dsl = Objects.requireNonNull(dsl);
        Objects.requireNonNull(identityInitializer);

        // The loading of system templates is intentionally done in the DaoImpl, to avoid having
        // methods to operate on default templates in the Dao interface. The alternative is
        // a loader class that works specifically with TemplatesDaoImpl.
        final Map<String, TemplateInfo> defaultTemplates = readTemplateInfos(defaultTemplatesFile);
        // We don't need retry logic, because Flyway migration and DSL context creation stalls
        // if the database is not up. There is the chance that the database will crash between
        // when we do flyway migration and context initialization and when we create
        // the TemplatesDaoImpl, but the likelihood of that is very small.
        replaceSystemTemplates(defaultTemplates);
    }

    /**
     * Get all existing templates.
     *
     * @return Set of all existing templates
     */
    @Nonnull
    @Override
    public Set<TemplateDTO.Template> getAllTemplates() {
        final List<Template> allTemplates = dsl.selectFrom(TEMPLATE).fetch().into(Template.class);
        return templatesToProto(allTemplates);
    }

    /**
     * Get the template by template id.
     *
     * @param id of template
     * @return Optional Template, if not found, will return Optional.empty().
     */
    @Nonnull
    @Override
    public Optional<TemplateDTO.Template> getTemplate(long id) {
        return getTemplate(dsl, id);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<TemplateDTO.Template> getTemplatesByName(@Nonnull final String name) {
        return dsl.selectFrom(TEMPLATE)
            .where(TEMPLATE.NAME.eq(name))
            .fetch().into(Template.class)
            .stream()
            .map(this::templateToProto)
            .collect(Collectors.toList());
    }

    /**
     * Create a new template
     *
     * @param templateInfo describe the contents of one template
     * @return new created template
     */
    @Override
    @Nonnull
    public TemplateDTO.Template createTemplate(@Nonnull final TemplateInfo templateInfo) {
        return internalCreateTemplate(dsl, templateInfo, TemplateDTO.Template.Type.USER);
    }

    @Nonnull
    private TemplateDTO.Template internalCreateTemplate(@Nonnull final DSLContext context,
                                                    @Nonnull final TemplateInfo templateInfo,
                                                    @Nonnull final TemplateDTO.Template.Type type) {
        final TemplateDTO.Template templateDto = TemplateDTO.Template.newBuilder()
                .setId(IdentityGenerator.next())
                .setType(type)
                .setTemplateInfo(templateInfo)
                .build();

        final TemplateRecord templateRecord = context.newRecord(TEMPLATE);
        updateRecordFromProto(templateDto, templateRecord);

        return templateDto;
    }

    /**
     * Update a existing template with new template instance.
     *
     * @param id of existing template
     * @param templateInfo the new template instance need to store
     * @return Updated template
     * @throws NoSuchObjectException if not found existing template.
     * @throws IllegalTemplateOperationException if any operation is not allowed.
     */
    @Override
    @Nonnull
    public TemplateDTO.Template editTemplate(long id, @Nonnull TemplateInfo templateInfo)
            throws NoSuchObjectException, IllegalTemplateOperationException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final TemplateRecord templateRecord = transactionDsl.selectFrom(TEMPLATE)
                        .where(TEMPLATE.ID.eq(id))
                        .fetchOne();
                if (templateRecord == null) {
                    throw noSuchObjectException(id);
                }

                final TemplateDTO.Template oldTemplate = templateToProto(templateRecord.into(Template.class));

                if (oldTemplate.getType() == Type.SYSTEM) {
                    // This may change in the future.
                    throw new IllegalTemplateOperationException(oldTemplate.getId(),
                            "System-created templates are not editable.");
                }

                final TemplateDTO.Template newTemplate = oldTemplate.toBuilder()
                        .setTemplateInfo(templateInfo)
                        .build();
                updateRecordFromProto(newTemplate, templateRecord);
                return newTemplate;
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e.getCause();
            } else if (e.getCause() instanceof IllegalTemplateOperationException) {
                throw (IllegalTemplateOperationException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete a existing template.
     *
     * @param id of existing template
     * @return Deleted template.
     * @throws NoSuchObjectException if not find existing template.
     * @throws IllegalTemplateOperationException If attempting to delete a template not created by the user.
     */
    @Nonnull
    @Override
    public TemplateDTO.Template deleteTemplateById(long id)
            throws NoSuchObjectException, IllegalTemplateOperationException {
        try {
            return dsl.transactionResult(configuration -> {
                //TODO: when delete a template, we need to delete relate reservations or modify related
                // reservations status to invalid.
                final DSLContext transactionDsl = DSL.using(configuration);
                final TemplateDTO.Template template = getTemplate(transactionDsl, id)
                        .orElseThrow(() -> noSuchObjectException(id));
                if (template.getType().equals(TemplateDTO.Template.Type.USER)) {
                    transactionDsl.deleteFrom(TEMPLATE).where(TEMPLATE.ID.eq(id)).execute();
                } else {
                    throw new IllegalTemplateOperationException(id,
                            "Only user-created templates can be deleted.");
                }
                return template;
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e.getCause();
            } else if (e.getCause() instanceof IllegalTemplateOperationException) {
                throw (IllegalTemplateOperationException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete discovered templates by target id.
     *
     * @param targetId id of target
     * @return all deleted Template object
     */
    @Nonnull
    @Override
    public List<TemplateDTO.Template> deleteTemplateByTargetId(long targetId) {
        return dsl.transactionResult(configuration -> {
            final DSLContext transactionDsl = DSL.using(configuration);
            final List<TemplateDTO.Template> deletedRecords = transactionDsl
                .selectFrom(TEMPLATE).where(TEMPLATE.TARGET_ID.eq(targetId)).fetch().into(Template.class)
                .stream()
                .map(this::templateToProto)
                .collect(Collectors.toList());
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
    @Nonnull
    @Override
    public Set<TemplateDTO.Template> getTemplatesByEntityType(int type) {
        final List<Template> templates = dsl.selectFrom(TEMPLATE)
            .where(TEMPLATE.ENTITY_TYPE.eq(type))
            .fetch()
            .into(Template.class);

        return templatesToProto(templates);
    }

    /**
     * Get a set of templates by template id list. The return templates size could be equal or less
     * than request ids size. The client needs to check if there are missing templates.
     *
     * @param ids Set of template ids.
     * @return  Set of templates.
     */
    @Override
    @Nonnull
    public Set<TemplateDTO.Template> getTemplates(@Nonnull Set<Long> ids) {
        final List<Template> templateList =
            dsl.selectFrom(TEMPLATE)
                .where(TEMPLATE.ID.in(ids)).fetch().into(Template.class);
        return templatesToProto(templateList);
    }

    /**
     * Get the count of matched templates which id is in the input id set.
     *
     * @param ids a set of template ids need check if exist.
     * @return the count of matched templates.
     */
    @Override
    public long getTemplatesCount(@Nonnull Set<Long> ids) {
        return dsl.fetchCount(dsl.selectFrom(TEMPLATE)
                .where(TEMPLATE.ID.in(ids)));
    }

    private Optional<TemplateDTO.Template> getTemplate(@Nonnull final DSLContext dsl,
                                                       final long id) {
        final TemplateRecord templateInstance =
            dsl.selectFrom(TEMPLATE).where(TEMPLATE.ID.eq(id)).fetchOne();

        return Optional.ofNullable(templateInstance)
            .map(templateRecord -> templateRecord.into(Template.class))
            .map(this::templateToProto);
    }

    private static NoSuchObjectException noSuchObjectException(final long id) {
        return new NoSuchObjectException("Template with id " + id + " not found");
    }

    @Nonnull
    private Set<TemplateDTO.Template> templatesToProto(@Nonnull List<Template> pojTemplates) {
        return pojTemplates.stream()
            .map(this::templateToProto)
            .collect(Collectors.toSet());
    }

    /**
     * Convert a JOOQ-generated {@link Template} to a {@link TemplateDTO.Template} object that
     * carries the same information.
     *
     * @param template The JOOQ-generated {@link Template} representing a row in the "template"
     *                table.
     * @return The {@link TemplateDTO.Template} that describes the template.
     */
    @Nonnull
    private TemplateDTO.Template templateToProto(@Nonnull final Template template) {
        final TemplateDTO.Template.Builder builder = TemplateDTO.Template.newBuilder()
                .setId(template.getId())
                .setTemplateInfo(template.getTemplateInfo());

        if (template.getTargetId() != null) {
            builder.setTargetId(template.getTargetId());
        }

        try {
            builder.setType(Type.valueOf(template.getType()));
        } catch (IllegalArgumentException e) {
            // This could happen if we change the names of the Type enum.
            final Type defaultType = Type.USER;
            logger.warn("Failed to interpret {} as a type. Should be one of: {}\nDefaulting to: {}",
                    template.getType(), StringUtils.join(Type.values(), ", "), defaultType);
            builder.setType(defaultType);
        }

        return builder.build();
    }

    /**
     * Analogous to the reverse of {@link this#templateToProto(Template)}. Overrides the fields in
     * a {@link TemplateRecord} (which represents a row in the "template" table) with information
     * from a {@link TemplateDTO.Template} proto, and stores the record back to the DB.
     *
     * @param template The template that represents the new information.
     * @param record The record to update. This is modified in this method, and the method will
     *               also call {@link TemplateRecord#store} to store the modified record
     *               in the database.
     */
    private void updateRecordFromProto(@Nonnull final TemplateDTO.Template template,
                                       @Nonnull final TemplateRecord record) {
        final TemplateInfo templateInfo = template.getTemplateInfo();
        record.setId(template.getId());
        record.setName(templateInfo.getName());
        record.setEntityType(templateInfo.getEntityType());
        if (template.hasTargetId()) {
            record.setTargetId(template.getTargetId());
        }
        record.setType(template.getType().name());
        if (templateInfo.hasProbeTemplateId()) {
            record.setProbeTemplateId(templateInfo.getProbeTemplateId());
        }
        record.setTemplateInfo(templateInfo);

        // This actually writes the record to the database.
        record.store();
    }

    @Nonnull
    private Map<String, TemplateInfo> readTemplateInfos(@Nonnull final String defaultTemplatesFile) {
        try (InputStream is = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(defaultTemplatesFile)) {
            logger.info("Loading default templates from file " + defaultTemplatesFile);
            final String dataJSON = IOUtils.toString(is, Charset.defaultCharset());
            final List<TemplateInfo> defaultTemplates = ComponentGsonFactory.createGson().fromJson(
                    dataJSON, new TypeToken<List<TemplateInfo>>() {}.getType());

            // Intentionally not providing a merger so that duplicate template names
            // result in a runtime exception.
            final Map<String, TemplateInfo> templatesByName =
                defaultTemplates.stream()
                    .collect(Collectors.toMap(TemplateInfo::getName, Function.identity()));

            if (!defaultTemplates.isEmpty()) {
                logger.info("Loaded default templates: {}", () -> defaultTemplates.stream()
                        .map(TemplateInfo::getName)
                        .collect(Collectors.joining(", ")));
            } else {
                logger.info("No default templates found in file " + defaultTemplatesFile);
            }
            return templatesByName;
        } catch (IOException e) {
            throw new RuntimeException("Unable to build default templates from file " + defaultTemplatesFile);
        }
    }

    /**
     * Replace the currently defined system-created templates with a new list.
     * All currently defined system-created templates will be deleted, and all modifications to
     * them will be lost.
     *
     * @param newTemplates The new templates. An empty list is acceptable - it means there should
     *                     be no more system templates.
     */
    private void replaceSystemTemplates(@Nonnull final Map<String, TemplateInfo> newTemplates) {
        dsl.transaction(configuration -> {
            final DSLContext transactionDsl = DSL.using(configuration);
            Result<TemplateRecord> existingRecords = transactionDsl.selectFrom(TEMPLATE)
                .where(TEMPLATE.TYPE.eq(Type.SYSTEM.name()))
                .fetch();
            final Set<String> existingNames = new HashSet<>();
            for (TemplateRecord record : existingRecords) {
                existingNames.add(record.getName());
                final TemplateInfo newInfo = newTemplates.get(record.getName());
                if (newInfo == null) {
                    logger.info("Deleting default template: {}" + record.getName());
                    record.delete();
                } else {
                    final TemplateDTO.Template existing = templateToProto(record.into(Template.class));
                    if (!newInfo.equals(existing.getTemplateInfo())) {
                        logger.info("Editing default template {} with new information.",
                                record.getName());
                        updateRecordFromProto(existing.toBuilder()
                            .setTemplateInfo(newInfo)
                            .build(), record);
                    } else {
                        logger.info("Default template {} is unchanged. Leaving it alone.",
                                record.getName());
                    }
                }
            }

            Sets.difference(newTemplates.keySet(), existingNames).forEach(nameToAdd -> {
                logger.info("Creating default template: {}", nameToAdd);
                internalCreateTemplate(transactionDsl, newTemplates.get(nameToAdd), Type.SYSTEM);
                logger.info("Created default template: {}", nameToAdd);
            });
        });
    }
}
