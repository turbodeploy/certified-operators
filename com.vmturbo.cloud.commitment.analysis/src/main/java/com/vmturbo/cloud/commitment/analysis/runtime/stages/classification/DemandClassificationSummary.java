package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value.Immutable;
import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;

/**
 * A utility class for creating a string summary of {@link DemandClassificationStage} results.
 */
@NotThreadSafe
public class DemandClassificationSummary {

    private static final String SUMMARY_TEMPLATE =
            "Total Allocated Demand Duration: <allocatedDemandDuration>\n"
                    + "Demand by Classification:\n"
                    + "    <classifiedAllocationSummary>\n";

    private static final String PER_CLASSIFICATION_TEMPLATE =
            "<classification> : Duration=<duration> (<percent>%)\n";

    private static final String DETAILED_SUMMARY_TEMPLATE =
            SUMMARY_TEMPLATE
                    + "Demand by Classification x Scope:\n"
                    + "<regionAccountSummary>";

    private static final String REGION_ACCOUNT_SCOPE_TEMPLATE =
            "    [Region=<region>, Account=<account>]\n"
                    + "        <scopedDemandSummary;separator=\"        \\n\">\n";

    private static final String SCOPED_DEMAND_TEMPLATE =
            "[Tier=<tier>(<tierDemand>) Classification=<classification>]: <duration>";

    private final Map<DemandClassification, Duration> allocatedDurationByClassification =
            new HashMap<>();

    private final Map<ClassifiedDemandScope, Duration> durationByDemandScope =
            new HashMap<>();

    private final MinimalCloudTopology<MinimalEntity> cloudTopology;

    private final boolean detailedSummary;


    private DemandClassificationSummary(@Nonnull MinimalCloudTopology<MinimalEntity> cloudTopology,
                                        boolean detailedSummary) {

        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.detailedSummary = detailedSummary;
    }

    /**
     * Constructs a new summary.
     * @param cloudTopology The cloud topology, consulted to determine the display names of referenced
     *                      entities within the collected demand.
     * @param detailedSummary Whether a detailed summary should be generated. If true, per demand
     *                        records are included in the summary.
     * @return A new instance of {@link DemandClassificationSummary}.
     */
    public static DemandClassificationSummary newSummary(
            @Nonnull MinimalCloudTopology<MinimalEntity> cloudTopology,
            boolean detailedSummary) {

        return new DemandClassificationSummary(cloudTopology, detailedSummary);
    }

    /**
     * Converts this summary to a collector, useful in summarizing data with a stream.
     * @return A {@link Consumer} of {@link ClassifiedEntityDemandAggregate} instances for allocated demand.
     */
    public Consumer<ClassifiedEntityDemandAggregate> toAllocatedSummaryCollector() {

        return (classifiedDemandAggregate) ->
                classifiedDemandAggregate.classifiedCloudTierDemand().forEach(
                        (classification, demandTimeSeriesSet) -> {

                            // First collect the total duration by classification
                            final Duration totalDuration = demandTimeSeriesSet.stream()
                                    .map(DemandTimeSeries::duration)
                                    .reduce(Duration.ZERO, Duration::plus);
                            allocatedDurationByClassification.compute(
                                    classification,
                                    (c, duration) -> (duration == null)
                                            ? totalDuration : duration.plus(totalDuration));

                            if (detailedSummary) {

                                demandTimeSeriesSet.forEach(demandTimeSeries -> {
                                    final ClassifiedDemandScope classifiedDemandScope =
                                            ImmutableClassifiedDemandScope.<AllocatedDemandClassification>builder()
                                                    .cloudTierDemand(demandTimeSeries.cloudTierDemand())
                                                    .accountOid(classifiedDemandAggregate.accountOid())
                                                    .regionOid(classifiedDemandAggregate.regionOid())
                                                    .classification(classification)
                                                    .build();

                                    durationByDemandScope.compute(
                                            classifiedDemandScope,
                                            (scope, duration) -> (duration == null)
                                                    ? demandTimeSeries.duration()
                                                    : duration.plus(demandTimeSeries.duration()));
                                });
                            }
                        });
    }

    /**
     * Generates a summary of the collected demand.
     * @return A summary of the collected demand.
     */
    @Override
    public String toString() {

        final Duration totalAllocatedDuration = allocatedDurationByClassification.values()
                .stream()
                .reduce(Duration.ZERO, Duration::plus);

        final ST template = new ST(detailedSummary ? DETAILED_SUMMARY_TEMPLATE : SUMMARY_TEMPLATE);

        template.add("allocatedDemandDuration", totalAllocatedDuration);
        template.add("classifiedAllocationSummary",
                allocatedDurationByClassification.entrySet()
                        .stream()
                        .map(e -> {
                            final DemandClassification classification = e.getKey();
                            final Duration duration = e.getValue();

                            final ST perClassificationTemplate = new ST(PER_CLASSIFICATION_TEMPLATE);

                            perClassificationTemplate.add("classification", classification);
                            perClassificationTemplate.add("duration", duration);
                            final double percent = duration.toMillis() * 100.0
                                    / totalAllocatedDuration.toMillis();
                            perClassificationTemplate.add("percent",
                                    String.format("%.2f", percent));

                            return perClassificationTemplate.render();
                        }).collect(Collectors.toList()));


        if (detailedSummary) {
            template.add("regionAccountSummary", toRegionAccountSummaries());
        }

        return template.render();
    }

    private Set<String> toRegionAccountSummaries() {
        final Table<Long, Long, List<Pair<ClassifiedDemandScope, Duration>>> durationByRegionAndAcct =
                durationByDemandScope.entrySet()
                        .stream()
                        .collect(ImmutableTable.toImmutableTable(
                                (e) -> e.getKey().regionOid(),
                                (e) -> e.getKey().accountOid(),
                                (e) -> ImmutableList.of(Pair.of(e.getKey(), e.getValue())),
                                ListUtils::sum));

        // The summaries are collected into a tree set to sort them
        Set<String> regionAcctSummaries = durationByRegionAndAcct.cellSet()
                .stream()
                .map(c -> {
                    final long regionOid = c.getRowKey();
                    final long accountOid = c.getColumnKey();
                    final Set<String> demandDurationSummaries = c.getValue()
                            .stream()
                            .map(e -> {
                                final ClassifiedDemandScope demandScope = e.getLeft();
                                final CloudTierDemand cloudTierDemand = demandScope.cloudTierDemand();
                                final Duration duration = e.getRight();

                                final ST demandSummary = new ST(SCOPED_DEMAND_TEMPLATE);

                                demandSummary.add("tier",
                                        cloudTopology.getEntity(cloudTierDemand.cloudTierOid())
                                                .map(MinimalEntity::getDisplayName)
                                                .orElse(StringUtils.EMPTY));
                                demandSummary.add("tierDemand", demandScope.cloudTierDemand());
                                demandSummary.add("classification", demandScope.classification());
                                demandSummary.add("duration", duration);

                                return demandSummary.render();
                            }).collect(Collectors.toCollection(TreeSet::new));

                    final ST regionAccountSummary = new ST(REGION_ACCOUNT_SCOPE_TEMPLATE);

                    regionAccountSummary.add("region",
                            cloudTopology.getEntity(regionOid)
                                    .map(MinimalEntity::getDisplayName)
                                    .orElse(StringUtils.EMPTY));
                    regionAccountSummary.add("account",
                            cloudTopology.getEntity(accountOid)
                                    .map(MinimalEntity::getDisplayName)
                                    .orElse(StringUtils.EMPTY));
                    regionAccountSummary.add("scopedDemandSummary", demandDurationSummaries);

                    return regionAccountSummary.render();
                }).collect(Collectors.toCollection(TreeSet::new));

        return regionAcctSummaries;
    }

    /**
     * A scoping class for detailed demand data.
     */
    @Immutable(lazyhash = true)
    abstract static class ClassifiedDemandScope {

        abstract CloudTierDemand cloudTierDemand();

        abstract long accountOid();

        abstract long regionOid();

        abstract DemandClassification classification();
    }
}
