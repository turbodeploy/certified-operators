package com.vmturbo.repository.graph.parameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Encapsulates configuration of an index in ArangoDB.
 *
 * @see <a href="https://www.arangodb.com/docs/3.4/indexing-which-index.html">ArangoDB Index Overview</a>
 */
public class IndexParameter {
    public static enum GraphIndexType {
        HASH,
        FULLTEXT,
        SKIPLIST
    }

    private final GraphIndexType indexType;

    private final List<String> fieldNames;

    private final String collectionName;

    private final boolean unique;

    // If the index is sparse, documents that have any null index field values will not included in
    // the index. (hash and skiplist indices only -- fulltext is always sparse)
    private final boolean sparse;

    // If false, then a uniqueness constraint error will be thrown if a non-unique index field is
    // added to the index. Defaults to true. Relevant to hash and skiplist indices only.
    private boolean deduplicate = true;

    private IndexParameter(final Builder builder) {
        this.indexType = builder.indexType;
        this.fieldNames = builder.fields;
        this.collectionName = builder.collectionName;
        this.unique = builder.unique;
        this.sparse = builder.sparse;
        this.deduplicate = builder.deduplicate;
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

    public boolean isSparse() {
        return sparse;
    }

    public boolean isDeduplicate() {
        return deduplicate;
    }

    public static class Builder {

        private final GraphIndexType indexType;

        private final String collectionName;

        private List<String> fields;

        private boolean unique;

        private boolean sparse;

        private boolean deduplicate;

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

        public Builder sparse(final boolean bool) {
            this.sparse = bool;
            return this;
        }

        public Builder deduplicate(final boolean bool) {
            this.deduplicate = bool;
            return this;
        }

        public IndexParameter build() {
            return new IndexParameter(this);
        }
    }
}
