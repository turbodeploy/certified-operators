package com.vmturbo.cost.component.diags;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;

/**
 * Class responsible for enabling/disabling foreign keys for cost. Used primarily
 * for when postgres is configured.
 */
class ForeignKeyHandler {
    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;
    private final List<ForeignConstraint> foreignConstraints;

    /**
     * ForeignKeyHandler constructor.
     *
     * @param dsl the dsl context.
     * @param foreignConstraints the list of foreign keys.
     */
    protected ForeignKeyHandler(@Nonnull DSLContext dsl,
            @Nonnull List<ForeignConstraint> foreignConstraints) {
        this.dsl = dsl;
        this.foreignConstraints = foreignConstraints;
    }

    /**
     * disables foreign keys.
     *
     * @throws DiagnosticsException if failed to disable foreign keys.
     */
    public void disableForeignKeys() throws DiagnosticsException {
        try {
            logger.info("Disabling cost foreign keys.");
            foreignConstraints.forEach(foreignConstraint -> {
                dsl.execute(String.format("ALTER TABLE \"%s\" DROP CONSTRAINT \"%s\"",
                        foreignConstraint.getTable(), foreignConstraint.getConstraint()));
            });
        } catch (Exception e) {
            throw new DiagnosticsException("Failed to disable cost foreign keys", e);
        }
    }

    /**
     * enables foreign keys.
     *
     * @throws DiagnosticsException if failed to disable foreign keys.
     */
    public void enableForeignKeys() throws DiagnosticsException {
        try {
            logger.info("Enabling cost foreign keys.");
            foreignConstraints.forEach(foreignConstraint -> {
                dsl.execute(String.format("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\" %s",
                        foreignConstraint.getTable(), foreignConstraint.getConstraint(),
                        foreignConstraint.getConstraintDefinition()));
            });
        } catch (Exception e) {
            throw new DiagnosticsException("Failed to enable cost foreign keys", e);
        }
    }
}
