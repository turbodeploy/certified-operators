package com.vmturbo.sql.utils;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.ExecuteContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultExecuteListener;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLExceptionTranslator;

/**
 * Translate exceptions from jOOQ to Spring-based exceptions.
 */
public class JooqExceptionTranslator extends DefaultExecuteListener {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Translate exceptions from jOOQ to Spring-based exceptions.
     * See https://blog.jooq.org/2012/09/19/a-nice-way-of-using-jooq-with-spring/
     *
     * @param context The execute context.
     */
    public void exception(ExecuteContext context) {
        SQLException sqlException = context.sqlException();
        logger.error("The SQL exception thrown when accessing the database using Jooq is: ",
                    sqlException);
        SQLDialect dialect = context.configuration().dialect();
        SQLExceptionTranslator translator
                = new SQLErrorCodeSQLExceptionTranslator(dialect.name());
        context.exception(translator
                .translate("Access database using jOOQ", context.sql(), sqlException));
    }
}
