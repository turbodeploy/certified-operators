package com.vmturbo.repository.graph.parameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class IndexParameter {
    public static enum GraphIndexType {
        HASH,
        FULLTEXT
    }

    private final GraphIndexType indexType;

    private final List<String> fieldNames;

    private final String collectionName;

    private final boolean unique;

    private IndexParameter(final Builder builder) {
        this.indexType = builder.indexType;
        this.fieldNames = builder.fields;
        this.collectionName = builder.collectionName;
        this.unique = builder.unique;
    }

    public GraphIndexType getIndexType() {
        return indexType;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public boolean isUnique() {
        return unique;
    }

    public static class Builder {

        private final GraphIndexType indexType;

        private final String collectionName;

        private List<String> fields;

        private boolean unique;

        public Builder(final String collectionName, final GraphIndexType indexType) {
            this.fields = new ArrayList<>();
            this.indexType = indexType;
            this.collectionName = collectionName;
            this.unique = false;
        }

        public Builder addField(final String fieldName) {
            this.fields.add(fieldName);
            return this;
        }

        public Builder addFields(final Collection<String> fields) {
            this.fields.addAll(fields);
            return this;
        }

        public Builder unique(final boolean bool) {
            this.unique = bool;
            return this;
        }

        public IndexParameter build() {
            return new IndexParameter(this);
        }
    }
}
