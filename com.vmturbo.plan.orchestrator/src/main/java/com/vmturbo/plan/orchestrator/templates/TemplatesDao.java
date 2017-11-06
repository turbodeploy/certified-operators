package com.vmturbo.plan.orchestrator.templates;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Data access object, responsible for creating, updating, searching, deleting templates.
 */
public interface TemplatesDao {
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
    Optional<Template> getTemplate(long id);

    /**
     * Create a new template to database and its template instance should be same as paramater
     * templateInstance.
     *
     * @param templateInstance describe the contents of one template
     * @return new created Template object
     */
    @Nonnull
    Template createTemplate(@Nonnull TemplateInfo templateInstance);

    /**
     * Update the existing template with a new template instance.
     *
     * @param id of existing template
     * @param templateInstance the new template instance need to store
     * @return new updated Template object
     * @throws NoSuchObjectException if can not find existing template
     */
    @Nonnull
    Template editTemplate(long id, @Nonnull TemplateInfo templateInstance) throws NoSuchObjectException;

    /**
     * Delete the existing template which template's ID equal to parameter id.
     *
     * @param id of existing template
     * @return deleted Template object
     * @throws NoSuchObjectException if can not find existing template
     */
    @Nonnull
    Template deleteTemplateById(long id) throws NoSuchObjectException;

    /**
     * Delete all discovered templates which belongs to parameter target id.
     *
     * @param targetId id of target
     * @return all deleted Template object
     */
    List<Template> deleteTemplateByTargetId(long targetId);

    /**
     * Get all templates which entity types equal to parameter type
     *
     * @param type entity type of template
     * @return all selected Template object
     */
    Set<Template> getTemplatesByType(int type);

    /**
     * Get a set of templates which id is in parameter list.
     * @param ids a list of template ids.
     * @return set of Template objects.
     */
    @Nonnull
    Set<Template> getTemplates(@Nonnull Set<Long> ids);
}
