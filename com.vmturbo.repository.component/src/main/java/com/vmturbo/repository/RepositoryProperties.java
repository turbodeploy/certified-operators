package com.vmturbo.repository;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("repository")
public class RepositoryProperties {

    final ArangoDB arangoDB = new ArangoDB();

    final Graphite graphite = new Graphite();

    public ArangoDB getArangodb() {
        return arangoDB;
    }

    public Graphite getGraphite() {
        return graphite;
    }

    // TODO : karthikt - All these config values should be moved
    // to consul. Also storing the root password for the DB is a bad idea.
    static class ArangoDB {
        String host = "127.0.0.1";

        int port = 8529;

        String defaultDatabase = "_system";

        String username = "root";

        String password = "root";

        String arangoDumpPath = "/home/turbonomic/arangodump";

        String arangoDumpOutputDir = "/home/turbonomic/data/arangodb-topology";

        String arangoRestorePath = "/home/turbonomic/arangorestore";

        String arangoRestoreBaseDir = "/home/turbonomic/data/arangodb-topology-received";

        String arangoRestoreInputDir = "/home/turbonomic/data/arangodb-topology";

        int batchSize = 100;

        int maxConnections = 5;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getDefaultDatabase() {
            return defaultDatabase;
        }

        public void setDefaultDatabase(String defaultDatabase) {
            this.defaultDatabase = defaultDatabase;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getArangoDumpPath() {
            return arangoDumpPath;
        }

        public String getArangoDumpOutputDir() {
            return arangoDumpOutputDir;
        }

        public String getArangoRestorePath() {
            return arangoRestorePath;
        }

        public String getArangoRestoreBaseDir() {
            return arangoRestoreBaseDir;
        }

        public String getArangoRestoreInputDir() {
            return arangoRestoreInputDir;
        }

        public String getServerEndpoint() {
            return "tcp://" + host + ":" + port;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getMaxConnections() {
            return this.maxConnections;
        }

        public void getMaxConnections(int maxConns) {
            this.maxConnections = maxConns;
        }
    }

    static class Graphite {
        String host = "graphite";

        int port = 2003;

        long reportingFreqMinutes = 1;

        boolean enable = false;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public long getReportingFreqMinutes() {
            return reportingFreqMinutes;
        }

        public void setReportingFreqMinutes(long reportingFreqMinutes) {
            this.reportingFreqMinutes = reportingFreqMinutes;
        }

        public boolean isEnable() {
            return enable;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }
    }
}
