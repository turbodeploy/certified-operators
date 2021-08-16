package com.vmturbo.sql.utils;

import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.ExecuteContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultExecuteListener;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLExceptionTranslator;

/**
 * Translate exceptions from jOOQ to Spring-based exceptions.
 */
public class JooqExceptionTranslator extends DefaultExecuteListener {
    private static final Logger logger = LogManager.getLogger();

    private final Function<String, SQLExceptionTranslator> translatorCreator;

    /**
     * Default constructor.
     */
    public JooqExceptionTranslator() {
        translatorCreator = d -> new SQLErrorCodeSQLExceptionTranslator(d);
    }

    /**
     * Creates an instance of {@link JooqExceptionTranslator} class that uses provided translator as
     * translator.
     *
     * @param translatorCreator the creator function for translator objects.
     */
    public  JooqExceptionTranslator(Function<String, SQLExceptionTranslator> translatorCreator) {
        this.translatorCreator = translatorCreator;
    }

    /**
     * Translate exceptions from jOOQ to Spring-based exceptions.
     * See https://blog.jooq.org/2012/09/19/a-nice-way-of-using-jooq-with-spring/
     *
     * @param context The execute context.
     */
    public void exception(ExecuteContext context) {
        SQLDialect dialect = context.configuration().dialect();
        SQLExceptionTranslator translator = translatorCreator.apply(dialect.name());
        logger.error("SQL exception while executing the query {}:",
                context.sql(), context.sqlException());
        if (context.sqlException() != null) {
            DataAccessException translatedException = translator
                    .translate("Access database using jOOQ", context.sql(), context.sqlException());
            if (translatedException != null) {
                logger.debug("The exception {} was translated to the following exception:",
                        context.sqlException(), translatedException);
                context.exception(translatedException);
            } else {
                logger.debug("The exception {} was translated to null.", context.sqlException());
                context.exception(new RuntimeException("SQL Exception occurred but cannot be translated.",
                        context.sqlException()));
            }
        }
    }
}
