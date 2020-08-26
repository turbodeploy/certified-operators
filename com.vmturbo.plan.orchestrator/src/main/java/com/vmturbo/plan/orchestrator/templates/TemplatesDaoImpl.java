package com.vmturbo.plan.orchestrator.templates;

import static com.vmturbo.plan.orchestrator.db.tables.ClusterToHeadroomTemplateId.CLUSTER_TO_HEADROOM_TEMPLATE_ID;
import static com.vmturbo.plan.orchestrator.db.tables.Template.TEMPLATE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.plan.TemplateDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Template;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateRecord;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;

/**
 * Implementation of {@link TemplatesDao} using underlying DB connection.
 */
public class TemplatesDaoImpl implements TemplatesDao {

    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

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
    public Set<TemplateDTO.Template> getFilteredTemplates(@Nonnull final TemplatesFilter filter) {
        // TODO (roman, Sept 20 2019) OM-50756: Add pagination parameters here (and all the way
        // up to the UI) to retrieve large sets of templates in chunks.
        final List<Template> allTemplates = dsl.selectFrom(TEMPLATE)
            .where(filterToConditions(filter))
            //TODO OM-52188 - temporarily hard-coding filtering results by name to avoid duplicate
            // templates being returned for cloud targets.
            // This should be removed when proper solution OM-52827 is implemented.
            .groupBy(TEMPLATE.NAME)
            .fetch()
            .into(Template.class);
        return templatesToProto(allTemplates);
    }

    @Nonnull
    private List<Condition> filterToConditions(@Nonnull final TemplatesFilter filter) {
        final List<Condition> conditions = new ArrayList<>();
        if (!filter.getTemplateIdsList().isEmpty()) {
            conditions.add(TEMPLATE.ID.in(filter.getTemplateIdsList()));
        }
        if (!filter.getTemplateNameList().isEmpty()) {
            conditions.add(TEMPLATE.NAME.in(filter.getTemplateNameList()));
        }
        if (filter.hasEntityType()) {
            conditions.add(TEMPLATE.ENTITY_TYPE.eq(filter.getEntityType()));
        }
        return conditions;
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
        return getTemplateInTransaction(dsl, id);
    }

    /**
     * Create a new template. If a template with the same name exists, update it.
     *
     * @param templateInfo describe the contents of one template
     * @return new created template
     * @throws DuplicateTemplateException if template name already exist
     */
    @Override
    @Nonnull
    public TemplateDTO.Template createTemplate(@Nonnull final TemplateInfo templateInfo) throws DuplicateTemplateException {
        return internalCreateTemplate(dsl, templateInfo, TemplateDTO.Template.Type.USER, Optional.empty());
    }

    /**
     * Create a new template. If a template with the same name exists, update it.
     *
     * @param templateInfo describe the contents of one template
     * @param targetId the target id this template is associated with
     * @return new created template
     * @throws DuplicateTemplateException if template name already exist
     */
    @Override
    @Nonnull
    public TemplateDTO.Template createTemplate(@Nonnull final TemplateInfo templateInfo,
                                               @Nonnull final Optional<Long> targetId) throws DuplicateTemplateException {
        return internalCreateTemplate(dsl, templateInfo, TemplateDTO.Template.Type.USER, targetId);
    }

    /**
     * Create a new template.
     *
     * @param context the transaction context
     * @param templateInfo describes the contents of one template
     * @param type of the template
     * @param targetId the target id this template is associated with
     * @return new created template
     */
    private TemplateDTO.Template createTemplate(@Nonnull final DSLContext context,
                                                @Nonnull final TemplateInfo templateInfo,
                                                @Nonnull final TemplateDTO.Template.Type type,
                                                @Nonnull final Optional<Long> targetId) {
        // Create a new template.
        final TemplateDTO.Template.Builder templateBuilder = TemplateDTO.Template.newBuilder()
            .setId(IdentityGenerator.next())
            .setType(type)
            .setTemplateInfo(templateInfo);
        targetId.ifPresent(templateBuilder::setTargetId);
        final TemplateDTO.Template template = templateBuilder.build();
        final TemplateRecord templateRecord = context.newRecord(TEMPLATE);
        updateRecordFromProto(template, templateRecord);
        return template;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public TemplateDTO.Template createOrEditTemplate(@Nonnull final TemplateInfo templateInfo,
                                                     @Nonnull final Optional<Long> targetId) {
        return dsl.transactionResult(configuration -> {
            Optional<TemplateRecord> templateRecordOptional =
                determineUniqueTemplate(dsl, templateInfo.getName(), null);

            if (templateRecordOptional.isPresent()) {
                // If a template with the same name exists, update it.
                return editTemplate(templateRecordOptional.get().getId(), templateInfo);
            } else {
                return createTemplate(dsl, templateInfo, TemplateDTO.Template.Type.USER, targetId);
            }
        });
    }

    /**
     * Check if a template with the given name and id exists in the db.
     * If id is null, check if a template with the given name exists in the db.
     * Otherwise, check if a template with the given name but a different id exists in the db.
     * If there's one template in the db satisfies the condition, return it.
     *
     * @param context Transaction context.
     * @param templateName template display name.
     * @param id           while editing a template.
     * @return an optional of TemplateRecord
     */
    private Optional<TemplateRecord> determineUniqueTemplate(
            final DSLContext context, String templateName, @Nullable Long id) {
        Condition existCondition = id == null ?
                TEMPLATE.NAME.eq(templateName) :
                TEMPLATE.NAME.eq(templateName).and(TEMPLATE.ID.notEqual(id));
        return Optional.ofNullable(context.fetchOne(TEMPLATE, existCondition));
    }

    /**
     * Create a new template. If a template with the same name exists, throw a DuplicateTemplateException.
     *
     * @param context the transaction context
     * @param templateInfo describes the contents of one template
     * @param type of the template
     * @param targetId the target id this template is associated with
     * @return new created template
     * @throws DuplicateTemplateException if duplicate template name found
     */
    @Nonnull
    private TemplateDTO.Template internalCreateTemplate(@Nonnull final DSLContext context,
                                                        @Nonnull final TemplateInfo templateInfo,
                                                        @Nonnull final TemplateDTO.Template.Type type,
                                                        @Nonnull final Optional<Long> targetId)
            throws DuplicateTemplateException {
        try {
            return dsl.transactionResult(configuration -> {
                Optional<TemplateRecord> templateRecordOptional =
                    determineUniqueTemplate(context, templateInfo.getName(), null);

                if (templateRecordOptional.isPresent()) {
                    // If a template with the same name exists, throw a DuplicateTemplateException.
                    throw new DuplicateTemplateException(templateInfo.getName());
                } else {
                    return createTemplate(context, templateInfo, type, targetId);
                }
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof DuplicateTemplateException) {
                throw (DuplicateTemplateException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Update a existing template with new template instance.
     *
     * @param id of existing template
     * @param templateInfo the new template instance need to store
     * @return Updated template
     * @throws NoSuchObjectException if not found existing template.
     * @throws IllegalTemplateOperationException if any operation is not allowed.
     * @throws DuplicateTemplateException if new template name already exist.
     */
    @Override
    @Nonnull
    public TemplateDTO.Template editTemplate(long id, @Nonnull TemplateInfo templateInfo)
            throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {
        return editTemplate(id, templateInfo, Optional.empty());
    }
    /**
     * Update a existing template with new template instance.
     *
     * @param id of existing template
     * @param templateInfo the new template instance need to store
     * @param targetId the target id this template is associated with
     * @return Updated template
     * @throws NoSuchObjectException if not found existing template.
     * @throws IllegalTemplateOperationException if any operation is not allowed.
     * @throws DuplicateTemplateException if new template name already exist.
     */
    @Override
    @Nonnull
    public TemplateDTO.Template editTemplate(long id, @Nonnull TemplateInfo templateInfo,
                                             @Nonnull final Optional<Long> targetId)
            throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);

                Optional<TemplateRecord> templateRecordOptional =
                    determineUniqueTemplate(transactionDsl, templateInfo.getName(), id);
                if (templateRecordOptional.isPresent()) {
                    throw new DuplicateTemplateException(templateInfo.getName());
                }

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

                final TemplateDTO.Template.Builder newTemplateBuilder = oldTemplate.toBuilder()
                        .setTemplateInfo(templateInfo);
                targetId.ifPresent(newTemplateBuilder::setTargetId);
                final TemplateDTO.Template newTemplate = newTemplateBuilder.build();
                updateRecordFromProto(newTemplate, templateRecord);
                return newTemplate;
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e.getCause();
            } else if (e.getCause() instanceof IllegalTemplateOperationException) {
                throw (IllegalTemplateOperationException)e.getCause();
            } else if (e.getCause() instanceof DuplicateTemplateException) {
                throw (DuplicateTemplateException)e.getCause();
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
                final TemplateDTO.Template template = getTemplateInTransaction(transactionDsl, id)
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
                throw (IllegalTemplateOperationException)e.getCause();
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

    @Nonnull
    @Override
    public Optional<TemplateDTO.Template> getClusterHeadroomTemplateForGroup(long groupId) {
        TemplateRecord templateInstance = dsl.selectFrom(TEMPLATE)
            .where(TEMPLATE.ID.eq(
                dsl.select(CLUSTER_TO_HEADROOM_TEMPLATE_ID.TEMPLATE_ID)
                    .from(CLUSTER_TO_HEADROOM_TEMPLATE_ID)
                    .where(CLUSTER_TO_HEADROOM_TEMPLATE_ID.GROUP_ID.eq(groupId))
            ))
            .fetchOne();

        return Optional.ofNullable(templateInstance)
            .map(templateRecord -> templateRecord.into(Template.class))
            .map(this::templateToProto);
    }

    @Override
    public void setOrUpdateHeadroomTemplateForCluster(long groupId, long templateId) {
        dsl.insertInto(CLUSTER_TO_HEADROOM_TEMPLATE_ID, CLUSTER_TO_HEADROOM_TEMPLATE_ID.GROUP_ID,
            CLUSTER_TO_HEADROOM_TEMPLATE_ID.TEMPLATE_ID)
            .values(groupId, templateId)
            .onDuplicateKeyUpdate()
            .set(CLUSTER_TO_HEADROOM_TEMPLATE_ID.TEMPLATE_ID, templateId)
            .execute();
    }

    private Optional<TemplateDTO.Template> getTemplateInTransaction(@Nonnull final DSLContext dsl, final long id) {
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
        final TemplateRecord toStore = addInfoToRecord(template, record);

        // This actually writes the record to the database.
        toStore.store();
    }

    /**
     * Overrides the fields in a {@link TemplateRecord} (which represents a row in the "template"
     * table) with information from a {@link TemplateDTO.Template} proto in preparation for
     * storing the record to the db.
     *
     * @param template The template that represents the new information.
     * @param record The record to update. This is modified in this method and returned.
     * @return the record parameter, modified ಠ_ಠ
     */
    private TemplateRecord addInfoToRecord(final @Nonnull TemplateDTO.Template template,
                                           final @Nonnull TemplateRecord record) {
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
        return record;
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
                try {
                    logger.info("Creating default template: {}", nameToAdd);
                    internalCreateTemplate(transactionDsl, newTemplates.get(nameToAdd), Type.SYSTEM, Optional.empty());
                    logger.info("Created default template: {}", nameToAdd);
                } catch (DuplicateTemplateException e) { // should never occur..
                    logger.error("Could not create default template : {} as another template with same name already exist.", nameToAdd);
                }
            });
        });
    }

    /**
     * {@inheritDoc}
     *
     * This method retrieves all templates and serializes them as JSON strings.
     *
     * @return a list of serialized templates
     * @throws DiagnosticsException on exceptions occurred
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender)
            throws DiagnosticsException {
        final Set<TemplateDTO.Template> templates =
            getFilteredTemplates(TemplatesFilter.getDefaultInstance());
        logger.info("Collecting diagnostics for {} templates", templates.size());
        for (TemplateDTO.Template template: templates) {
            appender.appendString(GSON.toJson(template, TemplateDTO.Template.class));
        }
    }

    /**
     * {@inheritDoc}
     *
     * This method clears all existing templates, then deserializes and adds a list of serialized
     * templates from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link StringDiagnosable#collectDiags(DiagnosticsAppender)}. Must be in the same order.
     * @throws DiagnosticsException if the db already contains templates, or in response
     *                              to any errors that may occur deserializing or restoring a
     *                              template.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {

        final List<String> errors = new ArrayList<>();

        final Set<TemplateDTO.Template> preexistingTemplates =
            getFilteredTemplates(TemplatesFilter.getDefaultInstance());
        if (!preexistingTemplates.isEmpty()) {
            final int numPreexisting = preexistingTemplates.size();
            final String clearingMessage = "Clearing " + numPreexisting +
                " preexisting templates: " +
                preexistingTemplates.stream().map(template -> template.getTemplateInfo().getName())
                    .collect(Collectors.toList());
            errors.add(clearingMessage);
            logger.warn(clearingMessage);

            final int deleted = deleteAllTemplates();
            if (deleted != numPreexisting) {
                final String deletedMessage = "Failed to delete " + (numPreexisting - deleted) +
                    " preexisting templates: " + getFilteredTemplates(TemplatesFilter.getDefaultInstance())
                    .stream()
                    .map(template -> template.getTemplateInfo().getName())
                    .collect(Collectors.toList());
                logger.error(deletedMessage);
                errors.add(deletedMessage);
            }
        }

        logger.info("Restoring {} serialized templates", collectedDiags.size());
        final long count = collectedDiags.stream().map(serialized -> {
            try {
                return GSON.fromJson(serialized, TemplateDTO.Template.class);
            } catch (JsonParseException e) {
                errors.add("Failed to deserialize template " + serialized +
                    " because of parse exception " + e.getMessage());
                return null;
            }
        }).filter(Objects::nonNull).map(this::restoreTemplate).filter(optional -> {
            optional.ifPresent(errors::add);
            return !optional.isPresent();
        }).count();
        logger.info("Successfully added {} templates to database", count);

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }

    }

    @Nonnull
    @Override
    public String getFileName() {
        return "Templates";
    }

    /**
     * Add a TemplateDTO.Template to the database.
     *
     * This is used when restoring serialized TemplateDTO.Templates from diagnostics and should
     * not be used for normal operations.
     *
     * @param template the Template to add
     * @return an optional of a string representing any error that may have occurred
     */
    private Optional<String> restoreTemplate(@Nonnull final TemplateDTO.Template template) {
        final TemplateRecord templateRecord = dsl.newRecord(TEMPLATE);
        try {
            int r = addInfoToRecord(template, templateRecord).store();
            return r == 1 ? Optional.empty() : Optional.of("Failed to restore template " + template);
        } catch (DataAccessException e) {
            return Optional.of("Could not restore template " + template +
                " because of DataAccessException " + e.getMessage());
        }
    }

    /**
     * Deletes all templates from the database.
     *
     * This is used when restoring serialized TemplateDTO.Templates from diagnostics and should
     * NOT be used for normal operations.
     *
     * @return the number of records deleted
     */
    private int deleteAllTemplates() {
        try {
            return dsl.deleteFrom(TEMPLATE).execute();
        } catch (DataAccessException e) {
            return 0;
        }
    }
}
