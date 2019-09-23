package com.vmturbo.plan.orchestrator.templates;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;

/**
 * Data access object, responsible for creating, updating, searching, deleting templates.
 */
public interface TemplatesDao extends Diagnosable {
    /**
     * Get all templates that match a filter.
     *
     * @param filter The filter to match.
     * @return Set of templates matching the filter.
     */
    @Nonnull
    Set<Template> getFilteredTemplates(@Nonnull TemplatesFilter filter);

    /**
     * Get one template which Template's ID is equal to parameter id.
     *
     * @param id of template
     * @return Optional template, if not found, it will be Optional.empty().
     */
    @Nonnull
    Optional<Template> getTemplate(long id);

    /**
     * Create a new template to database and its template instance should be same as paramater
     * templateInstance.
     *
     * @param templateInstance describe the contents of one template
     * @return new created Template object
     * @throws DuplicateTemplateException if not an unique template name.
     */
    @Nonnull
    Template createTemplate(@Nonnull TemplateInfo templateInstance) throws DuplicateTemplateException;

    /**
     * Update the existing template with a new template instance.
     *
     * @param id of existing template
     * @param templateInstance the new template instance need to store
     * @return new updated Template object
     * @throws NoSuchObjectException if can not find existing template
     * @throws IllegalTemplateOperationException If the operation is not allowed on this template.
     * @throws DuplicateTemplateException if not an unique template name.
     */
    @Nonnull
    Template editTemplate(long id, @Nonnull TemplateInfo templateInstance)
            throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException;

    /**
     * Delete the existing template which template's ID equal to parameter id.
     *
     * @param id of existing template
     * @return deleted Template object
     * @throws NoSuchObjectException if can not find existing template
     * @throws IllegalTemplateOperationException If the operation is not allowed on this template.
     */
    @Nonnull
    Template deleteTemplateById(long id)
            throws NoSuchObjectException, IllegalTemplateOperationException;

    /**
     * Delete all discovered templates which belongs to parameter target id.
     *
     * @param targetId id of target
     * @return all deleted Template object
     */
    @Nonnull
    List<Template> deleteTemplateByTargetId(long targetId);

    /**
     * Get the count of matched templates which id is in the input id set.
     *
     * @param ids a set of template ids need check if exist.
     * @return the count of matched templates.
     */
    long getTemplatesCount(@Nonnull Set<Long> ids);
}
