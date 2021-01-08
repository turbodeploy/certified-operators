package com.vmturbo.testbases.dropdead;

import static com.vmturbo.testbases.dropdead.DbObject.DbObjectType.EVENT;
import static com.vmturbo.testbases.dropdead.DbObject.DbObjectType.FUNCTION;
import static com.vmturbo.testbases.dropdead.DbObject.DbObjectType.PRODCEDURE;
import static com.vmturbo.testbases.dropdead.DbObject.DbObjectType.TABLE;
import static com.vmturbo.testbases.dropdead.DbObject.DbObjectType.VIEW;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;

/**
 * Representation of a DB object.
 */
public abstract class DbObject implements Comparable<DbObject> {

    private final DbObjectType type;
    private final String name;
    private final String definition;
    private Boolean dead;

    /**
     * Create a new instance.
     *
     * @param type       the type of this object
     * @param name       the name of this object
     * @param definition the definition of this object, if any
     */
    public DbObject(DbObjectType type, String name, @Nullable String definition) {
        this.type = type;
        this.name = name;
        this.definition = definition;
    }

    public DbObjectType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getDefinition() {
        return definition;
    }

    public boolean isDead() {
        return dead != null && dead;
    }

    public boolean isLive() {
        return dead != null && !dead;
    }

    public boolean isUndecided() {
        return dead == null;
    }

    void setDead() {
        this.dead = true;
    }

    void setLive() {
        this.dead = false;
    }

    abstract Collection<String> jooqNames(String jooqPackage);

    Collection<String> nonJooqNames() {
        // only try a non-jooq match if there's at least one underscore in the name, else
        // false matches are just too likely
        return name.contains("_")
                ? Collections.singletonList(name.toLowerCase())
                : Collections.emptyList();
    }

    abstract String dropStatement();

    @Override
    public int compareTo(final DbObject o) {
        return type == o.type ? name.compareTo(o.name) : type.compareTo(o.type);
    }

    /**
     * Types of objects we can in the DB schema.
     */
    enum DbObjectType {
        TABLE, VIEW, FUNCTION, PRODCEDURE, EVENT
    }

    protected String upperCamelName() {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name.toLowerCase());
    }

    protected String lowerCamelName() {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name.toLowerCase());
    }

    protected String upperSnakeName() {
        return name.toUpperCase();
    }

    /**
     * Class to represent tables appearing in the DB schema.
     */
    public static class TableObject extends DbObject {
        private List<String> jooqNames;

        /**
         * Create a new instance.
         *
         * @param name the table name
         */
        public TableObject(String name) {
            super(TABLE, name, null);
        }

        @Override
        public Collection<String> jooqNames(final String jooqPackage) {
            if (jooqNames == null) {
                this.jooqNames = ImmutableList.of(
                        jooqPackage + ".tables." + upperCamelName(),
                        "Tables." + upperSnakeName()
                );
            }
            return jooqNames;
        }

        @Override
        public String dropStatement() {
            return "DROP TABLE IF EXISTS " + getName();
        }
    }

    /**
     * Class to represent views appearing in the DB schema.
     */
    public static class ViewObject extends DbObject {
        private ImmutableList<String> jooqNames;

        /**
         * Create a new instance.
         *
         * @param name       name of the view
         * @param definition view definition (SELECT statement)
         */
        public ViewObject(String name, String definition) {
            super(VIEW, name, definition);

        }

        @Override
        public Collection<String> jooqNames(final String jooqPackage) {
            if (jooqNames == null) {
                this.jooqNames = ImmutableList.of(
                        jooqPackage + ".tables." + upperCamelName(),
                        "Tables." + upperSnakeName()
                );
            }
            return jooqNames;
        }

        @Override
        public String dropStatement() {
            return "DROP VIEW IF EXISTS " + getName();
        }
    }

    /**
     * Class to represent functions appearing in the DB schema.
     */
    public static class FunctionObject extends DbObject {
        private List<String> jooqNames;

        /**
         * Create a new instance.
         *
         * @param name       name of the function
         * @param definition definition
         */
        public FunctionObject(String name, String definition) {
            super(FUNCTION, name, definition);
        }

        @Override
        public Collection<String> jooqNames(final String jooqPackage) {
            if (jooqNames == null) {
                this.jooqNames = ImmutableList.of(
                        jooqPackage + ".routines." + upperCamelName(),
                        "Routines." + lowerCamelName()
                );
            }
            return jooqNames;
        }

        @Override
        public String dropStatement() {
            return "DROP FUNCTION IF EXISTS " + getName();
        }
    }

    /**
     * Class to represent stored procedures appearing in the DB schema.
     */
    public static class ProcObject extends DbObject {
        private ImmutableList<String> jooqNames;

        /**
         * Create a new instance.
         *
         * @param name       name of procedure
         * @param definition procedure definition
         */
        public ProcObject(String name, String definition) {
            super(PRODCEDURE, name, definition);
        }

        @Override
        public Collection<String> jooqNames(final String jooqPackage) {
            if (jooqNames == null) {
                this.jooqNames = ImmutableList.of(
                        jooqPackage + ".routines." + upperCamelName(),
                        "Routines." + lowerCamelName()
                );
            }
            return jooqNames;

        }

        @Override
        public String dropStatement() {
            return "DROP PROCEDURE IF EXISTS " + getName();
        }
    }

    /**
     * Class to represent events appearing in the DB schema.
     */
    public static class EventObject extends DbObject {
        /**
         * Create a new instance.
         *
         * @param name       event name
         * @param definition event definition
         */
        public EventObject(String name, String definition) {
            super(EVENT, name, definition);
        }

        @Override
        public Collection<String> jooqNames(final String jooqPackage) {
            return Collections.emptyList();
        }

        @Override
        public String dropStatement() {
            return "DROP EVENT IF EXISTS " + getName();
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DbObject dbObject = (DbObject)o;
        return type == dbObject.type && name.equals(dbObject.name) && Objects.equals(definition, dbObject.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, definition);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", type, name);
    }
}
