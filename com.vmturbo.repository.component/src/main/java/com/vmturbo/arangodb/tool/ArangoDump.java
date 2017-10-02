package com.vmturbo.arangodb.tool;

/**
 * Contains the command path and arguments needed to run the tool of dumping topology in Arango DB.
 *
 */
public class ArangoDump {
    /**
     * The server endpoint to connect to
     */
    private String endpoint;

    /**
     * The indicator to include system collections or not
     */
    private boolean systemCollections;

    /**
     * The output directory for the dumping files
     */
    private String outputDir;

    private ArangoDump(Builder b) {
        this.endpoint = b.endpoint;
        this.outputDir = b.outputDir;
        this.systemCollections = b.systemCollections;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public static class Builder {
        private String endpoint = "tcp://127.0.0.1:8529";

        private boolean systemCollections = true;

        private String outputDir = "/vmt-repo-topology";

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder outputDir(String outputDir) {
            this.outputDir = outputDir;
            return this;
        }

        public ArangoDump build() {
            return new ArangoDump(this);
        }
    }
}
