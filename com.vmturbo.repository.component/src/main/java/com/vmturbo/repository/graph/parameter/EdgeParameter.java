package com.vmturbo.repository.graph.parameter;

import java.util.List;

public class EdgeParameter {

    private String edgeCollection;

    private boolean waitForSync;

    private String from;

    private String to;

    private List<String> froms;

    private List<String> tos;

    private EdgeParameter(Builder b) {
        this.edgeCollection = b.edgeCollection;
        this.waitForSync = b.waitForSync;
        this.from = b.from;
        this.to= b.to;
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

        public Builder(String edgeCollection, String from, String to) {
            this.edgeCollection = edgeCollection;
            this.from = from;
            this.to = to;
        }

        public Builder waitFroSync() {
            this.waitForSync = true;
            return this;
        }

        public EdgeParameter build() {
            return new EdgeParameter(this);
        }
    }
}
