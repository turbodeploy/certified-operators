package com.vmturbo.reports.component.templates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.sql.utils.DbException;

/**
 * Controller to manupilate different reports templates at once. This controller has some {@link
 * TemplatesDao}s underlying to do the low-level work. Really, this class acts like a proxy to route
 * the template manipulation call to the appropriate template DAO.
 */
public class TemplatesOrganizer {

    private final Map<ReportType, TemplatesDao> templateSourceMap;

    /**
     * Constructs templates organizer with some specific templates DAOs.
     *
     * @param templatesDaos DAOs to access different report types
     */
    public TemplatesOrganizer(@Nonnull TemplatesDao... templatesDaos) {
        final ImmutableMap.Builder<ReportType, TemplatesDao> builder = new ImmutableMap.Builder<>();
        for (TemplatesDao dao: templatesDaos) {
            builder.put(dao.getReportType(), dao);
        }
        templateSourceMap = builder.build();
    }

    /**
     * Returns a collection of all the existing standard report templates.
     *
     * @return collection of templates.
     * @throws DbException if DB operation failed
     */
    @Nonnull
    public Collection<TemplateWrapper> getAllTemplates() throws DbException {
        final Collection<TemplateWrapper> result = new ArrayList<>();
        for (TemplatesDao templateDao : templateSourceMap.values()) {
            result.addAll(templateDao.getAllTemplates());
        }
        return result;
    }

    /**
     * Retrieves a report template record from the DB for the specified id, or nothing.
     *
     * @param reportType report type to use
     * @param templateId report template id to retrieve
     * @return report template record or empty optinoal.
     * @throws DbException if DB operation failed
     */
    @Nonnull
    public Optional<TemplateWrapper> getTemplateById(@Nonnull ReportType reportType, int templateId)
            throws DbException {
        final TemplatesDao templateSource = templateSourceMap.get(reportType);
        if (templateSource == null) {
            return Optional.empty();
        } else {
            return templateSource.getTemplateById(templateId);
        }
    }
}
