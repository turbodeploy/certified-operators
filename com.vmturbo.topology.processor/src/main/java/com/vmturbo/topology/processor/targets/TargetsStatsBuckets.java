package com.vmturbo.topology.processor.targets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsRequest.GroupBy;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsResponse.TargetsGroupStat;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsResponse.TargetsGroupStat.Builder;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsResponse.TargetsGroupStat.StatGroup;

/**
 * Represents a set of "buckets", where each bucket contains stats for target groups that
 * share a particular criteria.
 */
public class TargetsStatsBuckets {
    private final Map<GroupByBucketKey, TargetsStatsBucket> bucketForGroup = new HashMap<>();
    private final List<GroupBy> groupBy;

    /**
     * Constructor.
     *
     * @param groupBy targets grouping criteria
     */
    public TargetsStatsBuckets(@Nullable List<GroupBy> groupBy) {
        if (groupBy != null) {
            this.groupBy = groupBy;
        } else {
            this.groupBy = Collections.emptyList();
        }
    }

    /**
     * Populate stats for input targets.
     *
     * @param targets targets
     */
    public void addTargets(@Nonnull Collection<Target> targets) {
        targets.forEach(target -> {
            final GroupByBucketKey groupByBucketKey = getBucketKeyForTarget(target);
            bucketForGroup.computeIfAbsent(groupByBucketKey,
                    bucketKey -> new TargetsStatsBucket()).increaseTargetCount();
        });
    }

    /**
     * Return stats for targets combined into groups that share a particular criteria.
     *
     * @return list of {@link TargetsGroupStat}
     */
    public Collection<TargetsGroupStat> getTargetsStats() {
        return bucketForGroup.entrySet()
                .stream()
                .map(this::createTargetGroupStat)
                .collect(Collectors.toList());
    }

    private TargetsGroupStat createTargetGroupStat(
            @Nonnull Entry<GroupByBucketKey, TargetsStatsBucket> buckets) {
        final Builder targetsGroupStatBuilder = TargetsGroupStat.newBuilder();
        final GroupByBucketKey groupByBucketKey = buckets.getKey();
        if (groupByBucketKey.targetCategory().isPresent()) {
            targetsGroupStatBuilder.setStatGroup(StatGroup.newBuilder()
                    .setTargetCategory(groupByBucketKey.targetCategory().get())
                    .build());
        }
        targetsGroupStatBuilder.setTargetsCount(buckets.getValue().getTargetsCount());
        return targetsGroupStatBuilder.build();
    }

    private GroupByBucketKey getBucketKeyForTarget(@Nonnull final Target target) {
        final ImmutableGroupByBucketKey.Builder keyBuilder = ImmutableGroupByBucketKey.builder();
        if (groupBy.contains(GroupBy.TARGET_CATEGORY)) {
            keyBuilder.targetCategory(target.getProbeInfo().getUiProbeCategory());
        }
        return keyBuilder.build();
    }

    /**
     * Identifies the "key" of a bucket used to group targets stats.
     * The "key" is the unique combination of bucket identifiers.
     */
    @Value.Immutable
    public interface GroupByBucketKey {

        /**
         * The target category for this bucket.
         *
         * @return Value, or {@link Optional} if the requested group-by included the target
         * category.
         */
        Optional<String> targetCategory();
    }

    /**
     * "Bucket" containing targets stat for particular group.
     */
    private static class TargetsStatsBucket {
        private int targetsCount = 0;

        private TargetsStatsBucket() {
        }

        public void increaseTargetCount() {
            targetsCount++;
        }

        public int getTargetsCount() {
            return targetsCount;
        }
    }
}
