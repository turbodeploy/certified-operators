package com.vmturbo.repository.graph.parameter;

import java.util.List;

public class EdgeParameter {

    private String edgeCollection;

    private boolean waitForSync;

    private String from;

    private String to;

    private List<String> froms;

    private List<String> tos;

    private EdgeType edgeType;

    private EdgeParameter(Builder b) {
        this.edgeCollection = b.edgeCollection;
        this.waitForSync = b.waitForSync;
        this.from = b.from;
        this.to= b.to;
        this.edgeType = b.edgeType;
    }

    public String getEdgeCollection() {
        return edgeCollection;
    }

    public boolean isWaitForSync() {
        return waitForSync;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public List<String> getFroms() {
        return froms;
    }

    public List<String> getTos() {
        return tos;
    }

    /**
     * Get string representation of of this edge parameter.
     */
    public String getEdgeType() {
        return edgeType == null ? null : edgeType.name();
    }

    public EdgeParameter withFrom(String from) {
        this.from = from;
        return this;
    }

    public EdgeParameter withTo(String to) {
        this.to = to;
        return this;
    }

    public EdgeParameter withFroms(List<String> froms) {
        this.froms = froms;
        return this;
    }

    public EdgeParameter withTos(List<String> tos) {
        this.tos = tos;
        return this;
    }

    public static class Builder {

        private String edgeCollection;

        private boolean waitForSync = false;

        private String from;

        private String to;

        private EdgeType edgeType;

        public Builder(String edgeCollection, String from, String to, EdgeType edgeType) {
            this.edgeCollection = edgeCollection;
            this.from = from;
            this.to = to;
            this.edgeType = edgeType;
        }

        public Builder waitFroSync() {
            this.waitForSync = true;
            return this;
        }

        public EdgeParameter build() {
            return new EdgeParameter(this);
        }
    }

    /**
     * Enum representing the types of edges which are currently supported.
     */
    public enum EdgeType {
        /**
         * Consumes edge type, which is used for market relationship (one end of the edge is buying
         * commodities from the other end), including consumes and produces.
         */
        CONSUMES,

        /**
         * Connected edge type, which is used for connected to relationship (including normal
         * connected to and owns).
         */
        CONNECTED
    }
}
