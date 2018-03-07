package com.vmturbo.plan.orchestrator.templates;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Data access object, responsible for creating, updating, searching, deleting templates.
 */
public interface TemplatesDao extends Diagnosable {
    /**
     * Get all templates including user created and probe discovered templates.
     *
     * @return Set of templates
     */
    @Nonnull
    Set<Template> getAllTemplates();

    /**
     * Get one template which Template's ID is equal to parameter id.
     *
     * @param id of template
     * @return Optional template, if not found, it will be Optional.empty().
     */
    @Nonnull
    Optional<Template> getTemplate(final long id);

    /**
     * Get the templates that have a particular name.
     * TODO (roman, Nov 1 2017): Should template names be unique?
     *
     * @param name The name to look for.
     * @return The list of templates with that name. An empty list if there are none.
     */
    @Nonnull
    List<Template> getTemplatesByName(@Nonnull final String name);

    /**
     * Create a new template to database and its template instance should be same as paramater
     * templateInstance.
     *
     * @param templateInstance describe the contents of one template
     * @return new created Template object
     */
    @Nonnull
    Template createTemplate(@Nonnull final TemplateInfo templateInstance);

    /**
     * Update the existing template with a new template instance.
     *
     * @param id of existing template
     * @param templateInstance the new template instance need to store
     * @return new updated Template object
     * @throws NoSuchObjectException if can not find existing template
     * @throws IllegalTemplateOperationException If the operation is not allowed on this template.
     */
    @Nonnull
    Template editTemplate(final long id, @Nonnull final TemplateInfo templateInstance)
            throws NoSuchObjectException, IllegalTemplateOperationException;

    /**
     * Delete the existing template which template's ID equal to parameter id.
     *
     * @param id of existing template
     * @return deleted Template object
     * @throws NoSuchObjectException if can not find existing template
     * @throws IllegalTemplateOperationException If the operation is not allowed on this template.
     */
    @Nonnull
    Template deleteTemplateById(final long id)
            throws NoSuchObjectException, IllegalTemplateOperationException;

    /**
     * Delete all discovered templates which belongs to parameter target id.
     *
     * @param targetId id of target
     * @return all deleted Template object
     */
    @Nonnull
    List<Template> deleteTemplateByTargetId(final long targetId);

    /**
     * Get all templates which entity types equal to parameter type
     *
     * @param type entity type of template
     * @return all selected Template object
     */
    @Nonnull
    Set<Template> getTemplatesByEntityType(final int type);

    /**
     * Get a set of templates which id is in parameter list.
     * @param ids a list of template ids.
     * @return set of Template objects.
     */
    @Nonnull
    Set<Template> getTemplates(@Nonnull final Set<Long> ids);

    /**
     * Get the count of matched templates which id is in the input id set.
     *
     * @param ids a set of template ids need check if exist.
     * @return the count of matched templates.
     */
    long getTemplatesCount(@Nonnull final Set<Long> ids);
}
