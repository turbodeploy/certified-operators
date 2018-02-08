package com.vmturbo.reports.component.templates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.reports.db.abstraction.tables.records.OnDemandReportsRecord;
import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Controller to manupilate different reports templates at once. This controller has some {@link
 * TemplatesDao}s underlying to do the low-level work. Really, this class acts like a proxy to route
 * the template manipulation call to the appropriate template DAO.
 */
public class TemplatesOrganizer {

    private final Map<ReportType, TemplateSource<?>> templateSourceMap;

    /**
     * Constructs templates orginuzer with spme soecific templates DAOs.
     *
     * @param standardTemplatesDao DAO to access standard reports
     * @param onDemandTemplatesDao DAO to access on-demand reports
     */
    public TemplatesOrganizer(@Nonnull TemplatesDao<StandardReportsRecord> standardTemplatesDao,
            @Nonnull TemplatesDao<OnDemandReportsRecord> onDemandTemplatesDao) {
        final ImmutableMap.Builder<ReportType, TemplateSource<?>> builder =
                new ImmutableMap.Builder<>();
        builder.put(ReportType.BIRT_STANDARD,
                new TemplateSource<>(standardTemplatesDao, StandardTemplateWrapper::new));
        builder.put(ReportType.BIRT_ON_DEMAND,
                new TemplateSource<>(onDemandTemplatesDao, OnDemandTemplateWrapper::new));
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
        for (TemplateSource<?> templateSource : templateSourceMap.values()) {
            result.addAll(getAllTemplates(templateSource));
        }
        return result;
    }

    /**
     * Retrieves all the templates from the specific template source.
     *
     * @param templateSource templates source to use
     * @param <T> type of the template the source is holding
     * @return collection of protobuf representation of report templates
     * @throws DbException if DB exception occurs.
     */
    private static <T> Collection<TemplateWrapper> getAllTemplates(
            @Nonnull TemplateSource<T> templateSource) throws DbException {
        return templateSource.getTemplateDao()
                .getAllTemplates()
                .stream()
                .map(templateSource.getConverter())
                .collect(Collectors.toList());
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
        final TemplateSource<?> templateSource = templateSourceMap.get(reportType);
        if (templateSource == null) {
            return Optional.empty();
        } else {
            return getTemplateById(templateSource, templateId);
        }
    }

    private <T> Optional<TemplateWrapper> getTemplateById(@Nonnull TemplateSource<T> templateSource,
            int templateId) throws DbException {
        return templateSource.getTemplateDao()
                .getTemplateById(templateId)
                .map(templateSource.getConverter());
    }

    /**
     * Template source hold a apir of template DAO and converter to convert this specific template
     * DB record into protobuf representation.
     *
     * @param <T> type of DB records this source is pointing to
     */
    private class TemplateSource<T> {
        private final TemplatesDao<T> templateDao;
        private final Function<T, TemplateWrapper> converter;

        TemplateSource(@Nonnull TemplatesDao<T> templateDao,
                @Nonnull Function<T, TemplateWrapper> converter) {
            this.templateDao = Objects.requireNonNull(templateDao);
            this.converter = Objects.requireNonNull(converter);
        }

        @Nonnull
        public TemplatesDao<T> getTemplateDao() {
            return templateDao;
        }

        @Nonnull
        public Function<T, TemplateWrapper> getConverter() {
            return converter;
        }
    }
}
