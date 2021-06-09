package com.vmturbo.sql.utils.dbmonitor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Main component configuration for the Group Component. Manages groups and policies.
 */
@Configuration("dbMonitor")
public class DbMonitorConfig {

    /**
     * Default classification configuration, needs to be expanded over time.
     *
     * <p>Currently includes heavy-duty topology-ingestion activity:</p>
     * <ul>
     *     <li>Bulk upserts into stats tables</li>
     *     <li>Rollup activities</li>
     *     <li>Repartitioning activities</li>
     * </ul>
     *
     * <p>Also, the query used by this monitor is collected by it, and is configured to be classified
     * as "dbmonitor"</p>
     */
    private static final String DEFAULT_CLASSIFICATION = "---\n"
            // odd database name "?" is because that's that ProcessListRecord#getDb() returns
            // when the column value was null. In general, this would be a little more understandable
            // if it were a list of pattern/replacement structures, rather than an object. It would
            // also mean that we could rely on ordering of pattern match attempts, which we can't
            // now because JSON object properties are not considered to be ordered.
            + "\"?\":\n"
            + "  \".*`?information_schema`?.`?processlist`?\": dbmonitor\n"
            + "vmtdb:\n"
            // bulk loading of stats records
            + "  \"insert into `?(?<table>[a-z_0-9]+_latest)`?\": stats ingestion [${table}]\n"
            // rollups
            + "  \"(insert into|update) `?(?<table>[a-z_0-9]+_(by_hour|by_day|by_month))`?\": rollup [${table}]\n"
            // repartitioning
            + "  \"alter table `?(?<table>[a-z_0-9]+_(latest|by_hour|by_day|by_month))`?\": repartition [${table}]";

    private static Logger logger = LogManager.getLogger();

    /**
     * Classification structure for processlist entries.
     *
     * <p>The value should be a multiline string formatted as YAML. In a YAML file this is easily
     * created: simply define the property as a YAML structure, and then add "|-" on the line with
     * the property name, which turns the entire block into a block literal string.</p>
     *
     * <p>{@link #DEFAULT_CLASSIFICATION} is used if this is not overridden.</p>
     */
    @Value("${processListClassification:}")
    public String processListClassification;

    /**
     * This is potentially helpful when trying to create new classifications, since the default
     * classification is prone to including disparate queries in a single classification.
     *
     * <p>Default is false so we don't spam the logs too badly</p>
     */
    @Value("${dbMonitorDisableDefaultClassifications:false}")
    public boolean dbMonitorDisableDefaultClassifications;

    /**
     * Process list classifier.
     *
     * @return an instance of ProcessListClassifier
     * @throws JsonProcessingException exception
     */
    @Bean
    public ProcessListClassifier processListClassifier() throws JsonProcessingException {
        String classification = processListClassification;
        if (Strings.isNullOrEmpty(processListClassification)) {
            classification = DEFAULT_CLASSIFICATION;
        }
        return new ProcessListClassifier(classification, dbMonitorDisableDefaultClassifications);
    }
}
