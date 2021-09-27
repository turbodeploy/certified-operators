package com.vmturbo.search.metadata;

import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

/**
 * Describe how the search field is persisted.
 * To be used in schema creation and joins' construction.
 *
 * @param <DbValueT> database value type
 */
public class DbFieldDescriptor<DbValueT> {
    /**
     * How numerics are stored.
     */
    public static final DataType<Double> NUMERIC_DB_TYPE = SQLDataType.DOUBLE;
    /**
     * Truncated string length - backend does not store infinite strings.
     */
    public static final int STRING_SIZE = 1024;

    private final Location location;
    private final String column;
    // this is backend-specific i.e. jOOQ
    private final DataType<DbValueT> dbType;

    /**
     * Construct the field db storage description.
     *
     * @param location table
     * @param column column name
     * @param dbType db type e.g. "varchar"
     */
    public DbFieldDescriptor(Location location, String column, DataType<DbValueT> dbType) {
        this.location = location;
        this.column = column;
        this.dbType = dbType;
    }

    public Location getLocation() {
        return location;
    }

    public String getColumn() {
        return column;
    }

    public DataType<DbValueT> getDbType() {
        return dbType;
    }

    /**
     * Db model placement (tables).
     */
    public enum Location {
        /**
         * Entities table (with certain data fields also pulled up into it).
         */
        Entities("search_entity"),
        /**
         * Entity actions table.
         */
        Actions("search_entity_action"),
        /**
         * String and string list fields.
         */
        Strings("search_entity_string"),
        /**
         * Numeric and enum fields.
         */
        Numerics("search_entity_numeric"),
        /**
         * String map fields.
         */
        StringMaps("search_entity_string_map");

        private final String table;

        Location(String table) {
            this.table = table;
        }

        public String getTable() {
            return table;
        }
    }
}
