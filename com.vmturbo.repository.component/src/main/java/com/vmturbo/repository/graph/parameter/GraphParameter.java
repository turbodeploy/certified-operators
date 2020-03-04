package com.vmturbo.repository.graph.parameter;

import java.util.ArrayList;
import java.util.List;

public class GraphParameter {

    private String name;

    private boolean waitForSync;

    private List<EdgeDefParameter> edgeDefs;

    private int replicaCount;

    private GraphParameter(Builder b) {
        name = b.name;
        waitForSync = b.waitForSync;
        edgeDefs = b.edgeDefs;
        replicaCount = b.replicaCount;
    }

    public String getName() {
        return name;
    }

    public boolean isWaitForSync() {
        return waitForSync;
    }

    public List<EdgeDefParameter> getEdgeDefs() {
        return edgeDefs;
    }

    public int getReplicaCount() { return replicaCount; }

    public static class Builder {
        private String name;

        private boolean waitForSync = false;

        private List<EdgeDefParameter> edgeDefs = new ArrayList<>();

        private int replicaCount = 1;

        public Builder(String name) {
            this.name = name;
        }

        public Builder waitForSync() {
            waitForSync = true;
            return this;
        }

        public Builder addEdgeDef(EdgeDefParameter ed) {
            edgeDefs.add(ed);
            return this;
        }

        public Builder replicaCount(int newReplicaCount) {
            replicaCount = newReplicaCount;
            return this;
        }

        public GraphParameter build() {
            return new GraphParameter(this);
        }
    }
}
