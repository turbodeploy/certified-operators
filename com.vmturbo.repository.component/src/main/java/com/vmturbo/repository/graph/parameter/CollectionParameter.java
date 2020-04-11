package com.vmturbo.repository.graph.parameter;

public class CollectionParameter {

    // See ArangoDB documentation for the meaning of these parameters
    // https://www.arangodb.com/docs/3.4/http/document-working-with-documents.html#create-document
    private String name;

    private boolean isEdge;

    private boolean waitForSync;

    private int journalSize;

    private int numberOfShards;

    private int replicaCount;

    private CollectionParameter(Builder b) {
        name = b.name;
        isEdge = b.isEdge;
        waitForSync = b.waitForSync;
        journalSize = b.journalSize;
        numberOfShards = b.numberOfShards;
        replicaCount = b.replicaCount;
    }

    public int getJournalSize() {
        return journalSize;
    }

    public String getName() {
        return name;
    }

    public boolean isEdge() {
        return isEdge;
    }

    public boolean isWaitForSync() {
        return waitForSync;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getReplicaCount() { return replicaCount; }

    public static class Builder {
        private String name;

        private boolean isEdge = false;

        private boolean waitForSync = false;

        private int journalSize = 32 * 1024 * 1024; // 32 MB

        private int numberOfShards = 1;

        private int replicaCount = 1;

        public Builder(String name) {
            this.name = name;
        }

        public Builder waitForSync() {
            this.waitForSync = true;
            return this;
        }

        public Builder edge() {
            isEdge = true;
            return this;
        }

        public Builder journalSize(int size) {
            journalSize = size;
            return this;
        }

        public Builder numberOfShards(int num) {
            numberOfShards = num;
            return this;
        }

        public Builder replicaCount(int replicaCount) {
            this.replicaCount = replicaCount;
            return this;
        }

        public CollectionParameter build() {
            return new CollectionParameter(this);
        }
    }
}
