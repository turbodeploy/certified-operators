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
    public static class ActionSpec {

        private ActionSpec() {}

        /**
         * SPEC_OID column.
         */
        public static final Column<Long> SPEC_OID = Column.longColumn(Tables.ACTION_SPEC.SPEC_OID);

        /**
         * HASH column.
         */
        public static final Column<Long> HASH = Column.longColumn(Tables.ACTION_SPEC.HASH);

        /**
         * ACTION_TYPE column.
         */
        public static final Column<ActionType> TYPE = new Column<>(Tables.ACTION_SPEC.TYPE,
                ColType.ACTION_TYPE);

        /**
         * TARGET_ENTITY_ID. column
         */
        public static final Column<Long> TARGET_ENTITY = Column.longColumn(Tables.ACTION_SPEC.TARGET_ENTITY_ID);

        /**
         * INVOLVED_ENTITIES column.
         */
        public static final Column<Long[]> INVOLVED_ENTITIES = Column.longSetColumn(Tables.ACTION_SPEC.INVOLVED_ENTITIES.getName());
        /**
         * DESCRIPTION column.
         */
        public static final Column<String> DESCRIPTION = Column.stringColumn(Tables.ACTION_SPEC.DESCRIPTION);
        /**
         * SAVINGS column.
         */
        public static final Column<Double> SAVINGS = Column.doubleColumn(Tables.ACTION_SPEC.SAVINGS.getName());

        /**
         * ACTION_CATEGORY column.
         */
        public static final Column<ActionCategory> CATEGORY = new Column<>(Tables.ACTION_SPEC.CATEGORY, ColType.ACTION_CATEGORY);

        /**
         * ACTION_SEVERITY column.
         */
        public static final Column<Severity> SEVERITY = new Column<>(Tables.ACTION_SPEC.SEVERITY, ColType.SEVERITY);

        /**
         * FIRST_SEEN column.
         */
        public static final Column<Timestamp> FIRST_SEEN = Column.timestampColumn(Tables.ACTION_SPEC.FIRST_SEEN);

        /**
         * LAST_SEEN column.
         */
        public static final Column<Timestamp> LAST_SEEN = Column.timestampColumn(Tables.ACTION_SPEC.LAST_SEEN);

        /**
         * The ACTION_SPEC table.
         */
        public static final Table TABLE = Table.named(Tables.ACTION_SPEC.getName()).withColumns(
                SPEC_OID,
                HASH, TYPE, SEVERITY, CATEGORY,
                TARGET_ENTITY, INVOLVED_ENTITIES, DESCRIPTION,
                SAVINGS, FIRST_SEEN, LAST_SEEN).build();
    }

    /**
     * The ACTION table.
     */
    public static class ActionMetric {

        private ActionMetric() {}

        /**
         * The TIME column.
         */
        public static final Column<Timestamp> TIME = Column.timestampColumn(Tables.ACTION.TIME);

        /**
         * The STATE column.
         */
        public static final Column<ActionState> STATE = new Column<>(Tables.ACTION.STATE,
                ColType.ACTION_STATE);

        /**
         * The ACTION_OID column.
         */
        public static final Column<Long> ACTION_OID = Column.longColumn(Tables.ACTION.ACTION_OID);

        /**
         * The ACTION_SPEC_OID column.
         */
        public static final Column<Long> ACTION_SPEC_OID = Column.longColumn(Tables.ACTION.ACTION_SPEC_OID);

        /**
         * The ACTION_SPEC_HASH column.
         */
        public static final Column<Long> ACTION_SPEC_HASH = Column.longColumn(Tables.ACTION.ACTION_SPEC_HASH);

        /**
         * The USER column.
         */
        public static final Column<String> USER = Column.stringColumn(Tables.ACTION.USER);

        /**
         * The table.
         */
        public static final Table TABLE = Table.named(Tables.ACTION.getName()).withColumns(
                TIME, STATE, ACTION_OID, ACTION_SPEC_OID,
                ACTION_SPEC_HASH, USER).build();
    }

}
