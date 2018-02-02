package com.vmturbo.reports.component.templates;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.sql.utils.DbException;

/**
 * Abstract implementation of templates DAO, based on DB.
 *
 * @param <T> class of reports to retrieve.
 */
public abstract class AbstractDbTemplateDao<T> implements TemplatesDao<T> {

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

    @Nonnull
    protected <T> Collection<T> getAllReports(@Nonnull Class<T> pojoClass, @Nonnull Table<?> table)
            throws DbException {
        logger.debug("Getting all the report templates");
        final List<T> records;
        try {
            records = dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(table).fetch().into(pojoClass);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching all the reports", e);
        }
        return records;
    }

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
