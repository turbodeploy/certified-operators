package com.vmturbo.extractor.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class represents a database model - essentially a list of tables.
 */
public class Model {

    private final String name;
    private final List<Table> tables;

    /**
     * Private constructor to create a new model.
     *
     * <p>Client classes should use the {@link #named(String)} method to obtain a builder.</p>
     *
     * @param name   name of the model
     * @param tables tables appearing in the model
     */
    private Model(String name, List<Table> tables) {
        this.name = name;
        this.tables = tables;
    }

    /**
     * Create a builder for a model with the given name.
     *
     * @param name name of model
     * @return the new model builder
     */
    public static Builder named(String name) {
        return new Builder(name);
    }

    /**
     * Get this model's name.
     *
     * @return the model name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the tables belonging to this model.
     *
     * @return tables in model
     */
    public List<Table> getTables() {
        return tables;
    }

    /** Builder class for models. */
    public static class Builder {

        private final String name;
        private final List<Table> tables = new ArrayList<>();

        /**
         * Create a new builder instance for a model with the given name.
         *
         * @param name name of model
         */
        public Builder(String name) {
            this.name = name;
        }

        /**
         * Add tables to the model.
         *
         * @param tables tables to be added
         * @return this builder
         */
        public Builder withTables(Table... tables) {
            this.tables.addAll(Arrays.asList(tables));
            return this;
        }

        /**
         * Build the model.
         *
         * @return the new model
         */
        public Model build() {
            return new Model(name, tables);
        }
    }
}
