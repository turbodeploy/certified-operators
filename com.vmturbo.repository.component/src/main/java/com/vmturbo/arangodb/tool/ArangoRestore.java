package com.vmturbo.arangodb.tool;

/**
 * Contains the command path and arguments needed to run the tool of dumping topology in Arango DB.
 *
 */
public class ArangoRestore {
    /**
     * The server endpoint to connect to
     */
    private String endpoint;

    /**
     * The input directory for the dumping files
     */
    private String inputDir;

    /**
     * The base path for the input directory, which forms the path with the inputDir for the
     * input folder of the dumping files.
     *
     * E.g., baseDir = "/topology-received" and inputDir = "/arangodb-topology" make the
     * path of the input folder as "/topology-received/arangodb-topology"
     */
    private String baseDir;

    private ArangoRestore(Builder b) {
        this.endpoint = b.endpoint;
        this.inputDir = b.inputDir;
        this.baseDir = b.baseDir;
    }

    public String getInputDir() {
        return inputDir;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public static class Builder {
        private String endpoint = "tcp://127.0.0.1:8529";

        private String inputDir = "/vmt-repo-topology";

        private String baseDir = "";

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder inputDir(String inputDir) {
            this.inputDir = inputDir;
            return this;
        }

        public Builder baseDir(String baseDir) {
            this.baseDir = baseDir;
            return this;
        }

        public ArangoRestore build() {
            return new ArangoRestore(this);
        }
    }
}
