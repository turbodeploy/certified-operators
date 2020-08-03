package com.vmturbo.sql.utils;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Configuration;
import org.jooq.ExecuteContext;
import org.jooq.conf.Settings;
import org.jooq.conf.SettingsTools;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListener;

import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.OptScope;

/**
 * A simple listener to add JOOQ queries to an active trace.
 * Inspired by:
 * https://blog.jooq.org/2016/01/28/how-to-detect-slow-queries-with-jooq/
 */
public class JooqTracingInterceptor extends DefaultExecuteListener {
    private static final Logger logger = LogManager.getLogger();

    private OptScope scope = null;

    /**
     * A configuration to use to properly format the SQL queries for the tracing log.
     */
    private final Configuration formattingConfiguration;

    public JooqTracingInterceptor(@Nonnull final Configuration configuration) {
        this.formattingConfiguration = configuration.derive(
            SettingsTools.clone(configuration.settings()).withRenderFormatted(true));
    }

    @Override
    public void executeStart(ExecuteContext ctx) {
        if (scope != null) {
            // This shouldn't happen, because the various callbacks in the listener should
            // be called serially for each query execution.
            logger.warn("Unexpected - scope is not null when execution is started.");
        } else {
            scope = Tracing.addOpToTrace("jooq_query");
            if (ctx.query() != null) {
                Tracing.log(() -> DSL.using(formattingConfiguration).renderInlined(ctx.query()));
            }
        }
    }

    @Override
    public void exception(ExecuteContext ctx) {
        Tracing.log(() -> {
            if (ctx.sqlException() != null) {
                return "SQL Error: " + ctx.sqlException().getMessage();
            } else if (ctx.exception() != null) {
                return "Runtime Error: " + ctx.exception().getMessage();
            } else {
                return "Exception called in jOOQ interceptor.";
            }
        });
    }

    @Override
    public void executeEnd(ExecuteContext ctx) {
        if (scope != null) {
            scope.close();
            scope = null;
        }
    }
}
