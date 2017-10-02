package com.vmturbo.repository.graph.parameter;

import java.util.ArrayList;
import java.util.List;

public class GraphParameter {

    private String name;

    private boolean waitForSync;

    private List<EdgeDefParameter> edgeDefs;

    private GraphParameter(Builder b) {
        name = b.name;
        waitForSync = b.waitForSync;
        edgeDefs = b.edgeDefs;
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

    public static class Builder {
        private String name;

        private boolean waitForSync = false;

        private List<EdgeDefParameter> edgeDefs = new ArrayList<>();

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

        public GraphParameter build() {
            return new GraphParameter(this);
        }
    }
}
