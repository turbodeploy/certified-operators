package com.vmturbo.sql.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.ExecuteContext;
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
     * Create a new JooqTracingInterceptor. Do NOT use with the {@code DefaultExecuteListenerProvider}
     * because this will result in one TracingInterceptor per DSLContext where we actually want
     * one TracingInterceptor per {@link ExecuteContext}.
     */
    public JooqTracingInterceptor() {
    }

    @Override
    public void executeStart(ExecuteContext ctx) {
        super.executeStart(ctx);
        if (scope != null) {
            // This shouldn't happen, because we should create a  new TracingInterceptor
            // for every ExecuteContext.
            logger.error("Unexpected - scope is not null when execution is started.");
        } else {
            scope = Tracing.childOfActiveSpan("jooq_query");
            if (ctx.sql() != null) {
                Tracing.log(ctx::sql);
            }
        }
    }

    @Override
    public void exception(ExecuteContext ctx) {
        super.exception(ctx);
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
        super.executeEnd(ctx);
        if (scope != null) {
            scope.close();
            scope = null;
        }
    }
}
