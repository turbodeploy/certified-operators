package com.vmturbo.extractor.models;

import java.sql.Timestamp;

import com.vmturbo.extractor.schema.Tables;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.Severity;

/**
 * Definitions for action-related tables and columns for on-prem reporting.
 */
public class ActionModel {

    private ActionModel() {}

    /**
     * The ACTION_SPEC table.
     */
    public static class PendingAction {

        private PendingAction() {}

        /**
         * The ACTION_OID column.
         */
        public static final Column<Long> ACTION_OID = Column.longColumn(Tables.PENDING_ACTION.ACTION_OID);

        /**
         * ACTION_TYPE column.
         */
        public static final Column<ActionType> TYPE = new Column<>(Tables.PENDING_ACTION.TYPE,
                ColType.ACTION_TYPE);

        /**
         * TARGET_ENTITY_ID. column
         */
        public static final Column<Long> TARGET_ENTITY = Column.longColumn(Tables.PENDING_ACTION.TARGET_ENTITY_ID);

        /**
         * INVOLVED_ENTITIES column.
         */
        public static final Column<Long[]> INVOLVED_ENTITIES = Column.longSetColumn(Tables.PENDING_ACTION.INVOLVED_ENTITIES.getName());
        /**
         * DESCRIPTION column.
         */
        public static final Column<String> DESCRIPTION = Column.stringColumn(Tables.PENDING_ACTION.DESCRIPTION);

        /**
         * SAVINGS column.
         */
        public static final Column<Double> SAVINGS = Column.doubleColumn(Tables.PENDING_ACTION.SAVINGS.getName());

        /**
         * ACTION_CATEGORY column.
         */
        public static final Column<ActionCategory> CATEGORY = new Column<>(Tables.PENDING_ACTION.CATEGORY, ColType.ACTION_CATEGORY);

        /**
         * ACTION_SEVERITY column.
         */
        public static final Column<Severity> SEVERITY = new Column<>(Tables.PENDING_ACTION.SEVERITY, ColType.SEVERITY);

        /**
         * The ACTION_SPEC table.
         */
        public static final Table TABLE = Table.named(Tables.PENDING_ACTION.getName()).withColumns(
                ACTION_OID, TYPE, SEVERITY, CATEGORY,
                TARGET_ENTITY, INVOLVED_ENTITIES, DESCRIPTION,
                SAVINGS).build();
    }
}
