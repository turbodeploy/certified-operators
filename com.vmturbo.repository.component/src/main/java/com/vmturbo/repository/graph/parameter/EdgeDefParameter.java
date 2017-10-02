package com.vmturbo.repository.graph.parameter;

public class EdgeDefParameter {

    private String fromCollection;

    private String toCollection;

    private String edgeCollection;

    private EdgeDefParameter(Builder b) {
        fromCollection = b.fromCollection;
        toCollection = b.toCollection;
        edgeCollection = b.edgeCollection;
    }

    public String getFromCollection() {
        return fromCollection;
    }

    public String getToCollection() {
        return toCollection;
    }

    public String getEdgeCollection() {
        return edgeCollection;
    }

    public static class Builder {
        private String fromCollection;

        private String toCollection;

        private String edgeCollection;

        public Builder(String vertexCollection, String edgeCollection) {
            this.fromCollection = vertexCollection;
            this.toCollection = vertexCollection;
            this.edgeCollection = edgeCollection;
        }

        public Builder toCollection(String toCollection) {
            this.toCollection = toCollection;
            return this;
        }

        public EdgeDefParameter build() {
            return new EdgeDefParameter(this);
        }
    }
}
