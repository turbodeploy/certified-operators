package com.vmturbo.group.diagnostics;

import java.io.InputStream;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;

/**
 * The diags restore class which restores everthing in one transaction.
 */
public class TransactionalDiagnosticsHandlerImportable extends DiagnosticsHandlerImportable<DSLContext> {

    final DSLContext dslContext;

    /**
     * Constructs diagnostics handler.
     * @param zipReaderFactory zip reader factory to use when restoring.
     * @param diagnosable     diagnostics providers. If any of them are also importable, they
     * @param dslContext the DSL context for working with DB.
     */
    public TransactionalDiagnosticsHandlerImportable(@Nonnull DiagsZipReaderFactory zipReaderFactory,
                                                     @Nonnull Collection<?
                                                         extends Diagnosable> diagnosable,
                                                         DSLContext dslContext) {
        super(zipReaderFactory, diagnosable);
        this.dslContext = dslContext;
    }

    @Nonnull
    @Override
    public String restore(@Nonnull InputStream inputStream, DSLContext context) throws DiagnosticsException {
        StringBuilder sb = new StringBuilder();
        try {
            dslContext.transaction(config -> {
                final DSLContext transactionContext = DSL.using(config);
                sb.append(super.restore(inputStream, transactionContext));
            });
        } catch (DataAccessException ex) {
            throw new DiagnosticsException("Restoring component diagnostics fails as result of "
                + "database access error.", ex);
        }

        return sb.toString();
    }
}
