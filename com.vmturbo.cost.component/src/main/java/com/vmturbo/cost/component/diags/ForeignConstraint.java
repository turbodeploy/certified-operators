package com.vmturbo.cost.component.diags;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record;

/**
 * pojo to hold foreign key data (that's used for enabling/disabling foreign keys).
 */
class ForeignConstraint {
    private final String table;
    private final String constraint;
    private final String constraintDefinition;

    /**
     * ForeignConstraint constructor.
     *
     * @param table the table
     * @param constraint the constraint name
     * @param constraintDefinition the constraint definition (used to create the constraint)
     */
    protected ForeignConstraint(@Nonnull String table, @Nonnull String constraint,
            @Nonnull String constraintDefinition) {
        this.table = table;
        this.constraint = constraint;
        this.constraintDefinition = constraintDefinition;
    }

    @Nonnull
    public String getTable() {
        return table;
    }

    @Nonnull
    public String getConstraint() {
        return constraint;
    }

    @Nonnull
    public String getConstraintDefinition() {
        return constraintDefinition;
    }

    @Nonnull
    static List<ForeignConstraint> getForeignKeyConstraints(@Nonnull DSLContext dsl) {
        String query =
                "SELECT c.conrelid::regclass, c.conname, pg_get_constraintdef(c.oid)  FROM pg_constraint c, pg_namespace n "
                        + "WHERE c.connamespace=n.oid " + "AND n.nspname='cost' "
                        + "AND contype='f'";
        List<Record> records = dsl.fetch(query);
        return records.stream().map(record -> {
            String table = record.get("conrelid", String.class);
            String constraint = record.get("conname", String.class);
            String constraintDef = record.get("pg_get_constraintdef", String.class);
            return new ForeignConstraint(table, constraint, constraintDef);
        }).collect(Collectors.toList());
    }
}