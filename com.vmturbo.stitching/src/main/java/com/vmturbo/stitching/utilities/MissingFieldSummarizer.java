package com.vmturbo.stitching.utilities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class produces per-target summaries of missing fields during stitching.
 *
 * <p>When producing signatures for stitching we will need to retrieve the values of specific
 * fields. If those fields are missing then we need to produce a summarized log that will inform
 * as to which entities have are missing data.
 * </p>
 *
 */
public class MissingFieldSummarizer {

    private Logger logger;

    private Long targetId;
    private Map<EntityType, Map<String, SummaryEntry>> summary;

    private static MissingFieldSummarizer instance = null;

    private MissingFieldSummarizer(Logger logger) {
        this.logger = logger;
        this.summary = new HashMap<>();
    }

    private MissingFieldSummarizer() {
        this(LogManager.getLogger());
    }

    /**
     * Get the summarizer instance.
     * @return the summarizer
     */
    public static MissingFieldSummarizer getInstance() {
        if (instance == null) {
            instance = new MissingFieldSummarizer();
        }
        return instance;
    }

    @VisibleForTesting
    static MissingFieldSummarizer getInstance(Logger logger) {
        if (instance == null) {
            instance = new MissingFieldSummarizer(logger);
        }
        return instance;
    }

    /**
     * Set the target for which the summary is taken.
     *
     * @param targetId the id of the target
     */
    public void setTarget(Long targetId) {
        this.targetId = targetId;
    }

    /**
     * Add one more entry to be summarized.
     *
     * @param type type of the entity
     * @param field name of the missing field
     * @param localId the id of the entity
     */
    public void append(EntityType type, String field, String localId) {
        if (!summary.containsKey(type)) {
            summary.put(type, new HashMap<>());
        }
        summary.computeIfPresent(type, (t, m) -> {
            if (m.computeIfPresent(field, (f, v) -> v.append(localId)) == null) {
                m.put(field, new SummaryEntry().append(localId));
            }
            return m;
        });
    }

    /**
     * Produces the summary for the specified target. If no entries exist the summary will be empty
     * and nothing will be printed.
     */
    public void dump() {
        if (!summary.isEmpty()) {
            logger.warn(" ---- Missing field summary for target's {} stitching ----", targetId);
        }
        summary.forEach((type, rest) -> {
            rest.forEach((field, summaryEntry) -> {
                if (logger.isTraceEnabled()) {
                    logger.warn("{} entities of type {} are missing field {}. Sample ids: {}",
                            summaryEntry.getCounter(), type, field, summaryEntry.getLocalIds());
                } else {
                    logger.warn("{} entities of type {} are missing field {}.",
                            summaryEntry.getCounter(), type, field);
                }

            });
        });
    }

    /**
     * Clear target and summary information.
     */
    public void clear() {
        this.targetId = 0L;
        this.summary.clear();
    }

    /**
     * This is used to maintain a list of sample ids of entities that are missing a field,
     * together with the number of those entities. The sample ids will be shown if trace
     * is enabled.
     */
    private static class SummaryEntry {
        private final int maxIds = 5;

        private int counter;
        private final Set<String> localIds;

        SummaryEntry() {
            this.counter = 0;
            this.localIds = new HashSet<String>();
        }

        public SummaryEntry append(String oid) {
            if (this.counter < maxIds) {
                localIds.add(oid);
            }
            this.counter++;
            return this;
        }

        public int getCounter() {
            return counter;
        }

        public List<String> getLocalIds() {
            return Lists.newArrayList(localIds);
        }
    }
}
