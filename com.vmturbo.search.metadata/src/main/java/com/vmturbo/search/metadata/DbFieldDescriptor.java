package com.vmturbo.search.metadata;

import java.util.Objects;
import java.util.Set;

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

    // some fields e.g. oid and type go into more than one table - for pagination
    private final Set<Location> locations;
    private final String column;
    private final FieldPlacement placement;
    // this is backend-specific i.e. jOOQ
    private final DataType<DbValueT> dbType;

    /**
     * Construct the field db storage description.
     *
     * @param locations tables to contain that field
     * @param column column name
     * @param dbType db type e.g. "varchar"
     * @param placement how the field is stored within the table
     */
    public DbFieldDescriptor(Set<Location> locations, String column, DataType<DbValueT> dbType,
                    FieldPlacement placement) {
        this.locations = locations;
        this.column = column;
        this.dbType = dbType;
        this.placement = placement;
    }

    public Set<Location> getLocations() {
        return locations;
    }

    public String getColumn() {
        return column;
    }

    public DataType<DbValueT> getDbType() {
        return dbType;
    }

    public FieldPlacement getPlacement() {
        return placement;
    }

    @Override
    public int hashCode() {
        return Objects.hash(locations, column, dbType, placement);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DbFieldDescriptor<?> t = (DbFieldDescriptor)o;
        return Objects.equals(locations, t.locations) && Objects.equals(column, t.column)
                        && Objects.equals(dbType, t.dbType)
                        && Objects.equals(placement, t.placement);
    }

    @Override
    public String toString() {
        return "DbFieldDescriptor [locations=" + locations + ", column=" + column + ", placement="
                        + placement + "]";
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

        /**
         * ValueOf from table name, case insensitive.
         *
         * @param name table name
         * @return enum value, null if not found
         */
        public static Location fromName(String name) {
            for (Location loc : values()) {
                if (loc.getTable().equalsIgnoreCase(name)) {
                    return loc;
                }
            }
            return null;
        }
    }

    /**
     * Field placement inside a table.
     */
    public enum FieldPlacement {
        /**
         * A column in the Location table.
         */
        Column,
        /**
         * A row in the specific value-type Location table with predefined columns for oid/fieldname/fieldvalue.
         */
        Row,
        /**
         * Part of the composite predefined field ("attrs") in the Location table.
         */
        Json
    }
}
