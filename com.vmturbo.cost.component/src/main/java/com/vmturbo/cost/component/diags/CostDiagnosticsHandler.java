package com.vmturbo.cost.component.diags;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;

/**
 * Cost Diagnostics handler for importable diags. It extends {@link DiagnosticsHandler} with a
 * 'restore from diags' function.
 *
 * <p>If some of the restorable diagnosable failed to load its state from the associated file,
 * other diagnosable (and other files) will be tried nevertheless. As this is mostly a
 * debugging feature of XL, we'll try to restore as much as possible, regardless of consistency.
 * </p>
 *
 * <p>If the operation to disable foreign keys fails, then no diags will be loaded.</p>
 */
public class CostDiagnosticsHandler extends DiagnosticsHandlerImportable<DSLContext> {

    private final ForeignKeyHandler foreignKeyHandler;

    private final DSLContext dslContext;

    /**
     * Constructs diagnostics handler.
     *
     * @param zipReaderFactory zip reader factory to use when restoring.
     * @param collection diagnostics providers. If any of them are also importable, they
     * @param dsl the dsl context
     */
    public CostDiagnosticsHandler(@Nonnull DiagsZipReaderFactory zipReaderFactory,
            @Nonnull Collection collection, @Nonnull DSLContext dsl) {
        super(zipReaderFactory, collection);
        this.dslContext = dsl;
        this.foreignKeyHandler = (dslContext.dialect() == SQLDialect.POSTGRES)
                ? new ForeignKeyHandler(dsl, ForeignConstraint.getForeignKeyConstraints(dsl))
                : new ForeignKeyHandler(dsl, Collections.emptyList());
    }

    /**
     * Restore Cost component's state from diagnostics.
     *
     * @param inputStream diagnostics streamed bytes
     * @param context will be ignored, instead the context passed to the constructor will be used
     * @return list of errors if faced
     * @throws DiagnosticsException if diagnostics failed to restore
     */
    @Nonnull
    @Override
    public String restore(@Nonnull InputStream inputStream, @Nullable DSLContext context)
            throws DiagnosticsException {
        // This step is only necessary for Postgres
        if (dslContext.dialect() == SQLDialect.POSTGRES) {
            foreignKeyHandler.disableForeignKeys();
        }
        // Always pass the DSLContext injected through the constructor, which allows the
        // configuration of things like using unpooled connections for restoring diags.
        String result = super.restore(inputStream, dslContext);
        // This step is only necessary for Postgres
        if (dslContext.dialect() == SQLDialect.POSTGRES) {
            foreignKeyHandler.enableForeignKeys();
        }
        return result;
    }
}
