package com.vmturbo.repository;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RepositoryProperties {

    @Bean
    public ArangoDB getArangodb() {
        return new ArangoDB();
    }

    // TODO : karthikt - All these config values should be moved to consul.
    @Configuration
    public static class ArangoDB {

        @Value("${arangoDBNamespace:}")
        private String namespace;

        @Value("${REPOSITORY_ARANGODB_HOST:127.0.0.1}")
        private String host;

        @Value("${arangoDBPort:8529}")
        private int port;

        private String defaultDatabase = "_system";

        @Value("${arangoDBUsername:root}")
        private String username;

        @Value("${arangoDBPassword:}")
        private String password;

        private String arangoDumpPath = "/home/turbonomic/arangodump";

        private String arangoDumpOutputDir = "/home/turbonomic/data/arangodb-topology";

        private String arangoRestorePath = "/home/turbonomic/arangorestore";

        private String arangoRestoreBaseDir = "/home/turbonomic/data/arangodb-topology-received";

        private String arangoRestoreInputDir = "/home/turbonomic/data/arangodb-topology";

        private int batchSize = 100;

        private int maxConnections = 5;

        /**
         * Get the ArangoDB namespace to be used as prefix to construct database names to support
         * multi tenancy.
         *
         * @return ArangoDB namespace to be used as prefix to construct database names.
         */
        public String getNamespace() {
            return namespace;
        }

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

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
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
}
