package com.vmturbo.reports.component.templates;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.sql.utils.DbException;

/**
 * Data access object for reporting templates.
 */
public interface TemplatesDao {

    @Nonnull
    ReportType getReportType();

    /**
     * Returns a collection of all the existing standard report templates.
     *
     * @return collection of templates.
     * @throws DbException if DB operation failed
     */
    @Nonnull
    Collection<TemplateWrapper> getAllTemplates() throws DbException;

    /**
     * Retrieves a report template record from the DB for the specified id, or nothing.
     *
     * @param templateId report template id to retrieve
     * @return report template record or empty optinoal.
     * @throws DbException if DB operation failed
     */
    @Nonnull
    Optional<TemplateWrapper> getTemplateById(int templateId) throws DbException;
}
