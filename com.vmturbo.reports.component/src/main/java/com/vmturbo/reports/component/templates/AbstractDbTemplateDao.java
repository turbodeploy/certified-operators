package com.vmturbo.reports.component.templates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

import com.vmturbo.history.schema.abstraction.tables.records.ReportAttrsRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Abstract implementation of templates DAO, based on DB.
 *
 * @param <T> class of reports to retrieve.
 */
public abstract class AbstractDbTemplateDao<T extends Record> implements TemplatesDao {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Database access context.
     */
    private final DSLContext dsl;

    /**
     * Constructs templates DAO.
     *
     * @param dsl DSL context to use
     */
    protected AbstractDbTemplateDao(@Nonnull DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * Creates collection of report template wrappers from the grouped result of SQL query.
     *
     * @param groupedResults grouped result of SQL query
     * @return collection of template wrappers
     */
    @Nonnull
    protected Collection<TemplateWrapper> mapResults(
            @Nonnull Map<T, Result<Record>> groupedResults) {
        final Collection<TemplateWrapper> result = new ArrayList<>(groupedResults.size());
        for (Entry<T, Result<Record>> entry : groupedResults.entrySet()) {
            result.add(mapResult(entry));
        }
        return Collections.unmodifiableCollection(result);
    }

    /**
     * Converts a single report into a template wrapper. This is an utility method, as JOOQ
     * returns collection of results for every request.
     *
     * @param records map of grouped results. May be empty.
     * @return report template wrapper, if any, or Optional#empty
     * @throws DbException if there is more then 1 result in the data set supplied
     */
    @Nonnull
    protected Optional<TemplateWrapper> mapOneLineResult(@Nonnull Map<T, Result<Record>> records)
            throws DbException {
        if (records.size() > 1) {
            throw new DbException("Multiple reports returned for a single-report query " + records
                    .keySet()
                    .stream()
                    .map(record -> record.get(0))
                    .collect(Collectors.toList()));
        } else if (records.isEmpty()) {
            return Optional.empty();
        } else {
            final Entry<T, Result<Record>> record = records.entrySet().iterator().next();
            return Optional.of(mapResult(record));
        }
    }

    @Nonnull
    private TemplateWrapper mapResult(@Nonnull Entry<T, Result<Record>> entry) {
        final List<ReportAttrsRecord> attributes = entry.getValue()
                .stream()
                .map(attr -> attr.into(ReportAttrsRecord.class))
                .filter(attr -> attr.getId() != null)
                .collect(Collectors.toList());
        return wrapTemplate(entry.getKey(), attributes);
    }

    /**
     * Method create a report template wrapper based on the specified template and list of report
     * attributes
     *
     * @param template template discovered
     * @param attributes template attributes list
     * @return template wrapper
     */
    @Nonnull
    protected abstract TemplateWrapper wrapTemplate(@Nonnull T template,
            @Nonnull List<ReportAttrsRecord> attributes);

    /**
     * Returns DSL context to use by the class.
     *
     * @return DSL context
     */
    protected DSLContext getDsl() {
        return dsl;
    }

    /**
     * Returns logger to use.
     *
     * @return logger to use
     */
    protected Logger getLogger() {
        return logger;
    }
}
