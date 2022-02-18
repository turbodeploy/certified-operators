package com.vmturbo.cost.component.diags;

import java.io.InputStream;
import java.util.Collection;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

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
 *
 * @param <T> the type of context object.
 */
public class CostDiagnosticsHandler<T> extends DiagnosticsHandlerImportable<T> {

    private final ForeignKeyHandler foreignKeyHandler;

    /**
     * Constructs diagnostics handler.
     *
     * @param zipReaderFactory zip reader factory to use when restoring.
     * @param collection diagnostics providers. If any of them are also importable, they
     * @param dsl the dsl context
     */
    public CostDiagnosticsHandler(@NotNull DiagsZipReaderFactory zipReaderFactory,
            @NotNull Collection collection, @NotNull DSLContext dsl) {
        super(zipReaderFactory, collection);
        this.foreignKeyHandler = new ForeignKeyHandler(dsl,
                ForeignConstraint.getForeignKeyConstraints(dsl));
    }

    @NotNull
    @Override
    public String restore(@NotNull InputStream inputStream, @NotNull T context)
            throws DiagnosticsException {
        foreignKeyHandler.disableForeignKeys();
        String result = super.restore(inputStream, context);
        foreignKeyHandler.enableForeignKeys();
        return result;
    }
}
