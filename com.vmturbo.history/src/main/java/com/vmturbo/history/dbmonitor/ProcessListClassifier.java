package com.vmturbo.history.dbmonitor;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class that can be used to classify records obtained from the MySQL information_schema.processlist
 * table.
 *
 * <p>Classifying records can make them more understandable in the logs, and also permits summary
 * logging of multiple similar threads.</p>
 */
class ProcessListClassifier {
    private static Logger logger = LogManager.getLogger();
    private final Classification classification;

    ProcessListClassifier(String classification, boolean disableDefaultClassifications)
            throws JsonProcessingException {
        this.classification = new Classification(classification, disableDefaultClassifications);
    }

    Optional<Object> classify(ProcessListRecord record) {
        return classification.classify(record);
    }

    /**
     * A representation of the classification specification configured for the db monitor.
     *
     * <p>The configuration consists of multiple regex/replacement pairs, represented as a map.
     * An outer map permits each schema to have its own configurations.</p>
     *
     * <p>To classify a thread, its `info` value from the `processlist` table is matched against
     * the patterns configured for the schema associated with the thread, and if there's a match,
     * its replacement string (a la {@link String#replaceAll(String, String)} is used to create
     * a label for the classification.</p>
     *
     * <p>A fallback classification is constructed from the `command` and `db` column values from
     * processlist table.</p>
     */
    static class Classification {

        // map: {schema-name => {regex pattern => replacement string}}
        private final ImmutableMap<String, Map<Pattern, String>> classification;
        private final boolean disableDefaultClassfication;

        Classification(String yaml, boolean disableDefaultClassfication) throws JsonProcessingException {
            final YAMLMapper yamlMapper = new YAMLMapper();
            final Map<String, Map<String, String>> map =
                    yamlMapper.convertValue(yamlMapper.readTree(yaml), Map.class);
            this.classification = map.entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            Entry::getKey,
                            e -> createPatternMap(e.getValue())));
            this.disableDefaultClassfication = disableDefaultClassfication;
        }

        private Map<Pattern, String> createPatternMap(Map<String, String> map) {
            return map.entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            e -> Pattern.compile("^" + e.getKey() + ".*$"),
                            Entry::getValue));
        }

        Optional<Object> classify(ProcessListRecord record) {
            final String db = record.getDb();
            String cls = Optional.ofNullable(classification.get(db))
                    .flatMap(patMap -> classify(record, patMap))
                    .orElse(disableDefaultClassfication ? null
                            : classifyWithoutInfo(record));
            return Optional.ofNullable(cls);
        }

        private Optional<String> classify(ProcessListRecord record, Map<Pattern, String> patternMap) {
            return patternMap.entrySet().stream()
                    .map(e -> classify(record, e.getKey(), e.getValue()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst();
        }

        private Optional<String> classify(ProcessListRecord record, Pattern pattern, String replacement) {
            String info = record.getTrimmedInfo(1000);
            String db = record.getDb();
            if (info != null) {
                info = info.toLowerCase();
                Matcher m = pattern.matcher(info);
                return m.matches() ? Optional.of(info.replaceAll(pattern.toString(), replacement)) : Optional.empty();
            } else {
                return Optional.empty();
            }
        }

        private String classifyWithoutInfo(ProcessListRecord record) {
            return String.format("%s/%s", record.getCommand(), record.getDb());
        }
    }

    /**
     * A mostly-POJO that represents selected fields of a processlist table record.
     */
    static class ProcessListRecord {
        private String db;
        private long time;
        private String command;
        private String state;
        private String info;

        public String getDb() {
            return db;
        }

        public void setDb(final String db) {
            this.db = db != null ? db : "?";
        }

        public long getTime() {
            return time;
        }

        public void setTime(final Long time) {
            this.time = time != null ? time : 0L;
        }

        public String getCommand() {
            return command;
        }

        public void setCommand(final String command) {
            this.command = command != null ? command : "?";
        }

        public String getState() {
            return state;
        }

        public void setState(final String state) {
            this.state = state != null ? state : "?";
        }

        public String getInfo() {
            return info;
        }

        public String getTrimmedInfo(int n) {
            if (info != null) {
                String trimmed = info.replace("\n", " ").replaceAll("\\s+", " ");
                return trimmed.length() > n ? trimmed.substring(0, n) + "..." : trimmed;
            } else {
                return null;
            }
        }

        public void setInfo(final String info) {
            this.info = info;
        }
    }
}

