package com.vmturbo.topology.processor.staledata;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.TextFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;

/**
 * Basic Consumer of Stale Data reports that creates a summary and logs at INFO level.
 */
public class StaleDataLoggingConsumer implements StaleDataConsumer {
    private static final Logger logger = LogManager.getLogger();
    private final String tableFormat = "| %40s | %16s | %20s | %20s |";

    @Override
    public void accept(@Nonnull Map<Long, TargetHealth> targetToTargetHealth) {

        List<Entry<Long, TargetHealth>> unhealthyTargets = targetToTargetHealth
                .entrySet()
                .stream()
                .filter(entry -> (entry.getValue().hasErrorType() || !entry.getValue().getErrorTypeInfoList().isEmpty()))
                .collect(Collectors.toList());

        String header = getHeader();

        String body = unhealthyTargets.stream().map(entry -> {
            Long targetId = entry.getKey();
            TargetHealth health = entry.getValue();

            return String.format(tableFormat, health.getTargetName(), targetId,
                    health.getSubcategory().name(), health.getErrorType().name());
        }).collect(Collectors.joining("\n"));

        if (!body.isEmpty()) {

            if (logger.isDebugEnabled()) {
                String completeTargetHealth = unhealthyTargets
                        .stream()
                        .map(Entry::getValue)
                        .map(TextFormat::shortDebugString)
                        .collect(Collectors.joining("\n"));
                body += "\n == Verbose Target health ==\n" + completeTargetHealth
                        + "\n == end of verbose section";
            }
            logger.info(" == Stale Data Report ==\n" + header + body);

            logger.info(" == end of report ==");
        }
    }

    @Nonnull
    private String getHeader() {
        return String.format(tableFormat + "\n", "Target Name", "Oid", "Sub-Category", "Error");
    }
}
