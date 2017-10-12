package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.api.dto.input.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.input.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.reports.db.RelationType;

/**
 * Maps stats snapshots between their API DTO representation and their protobuf representation.
 */
public class StatsMapper {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    public static final String RELATION_FILTER_TYPE = "relation";

    private static final ImmutableMap<String, Optional<String>> dbToUiStatTypes = ImmutableMap.of(
               RelationType.COMMODITIES.getLiteral(), Optional.of("sold"),
               RelationType.COMMODITIESBOUGHT.getLiteral(), Optional.of("bought"),
               // (June 12, 2017): "attribute" is not a relation that the UI understands,
               // so don't map it to any relation type.
               RelationType.COMMODITIES_FROM_ATTRIBUTES.getLiteral(), Optional.empty(),
               // (June 8, 2017): "plan" is not valid relation type from the UI's point of view,
               // so don't map it to any relation type when constructing results for the UI.
               "plan", Optional.empty());

    /**
     * Convert a protobuf Stats.StatSnapshot to an API DTO StatSnapshotApiDTO.
     *
     * A Snapshot consists of a date, a date range, and a collection of SnapshotRecords.
     *
     * The collection may be zero length.
     *
     * @param statSnapshot a {@link StatSnapshot} protobuf to be converted to a {@link StatSnapshotApiDTO} for
     *                     return to the REST API caller (e.g. UX)
     * @return a {@link StatSnapshotApiDTO} with fields initialized from the given StatSnapshot
     *
     **/
    public static StatSnapshotApiDTO toStatSnapshotApiDTO(StatSnapshot statSnapshot) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        dto.setDate(statSnapshot.getSnapshotDate());
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                .map(StatsMapper::toStatApiDto)
                .collect(Collectors.toList()));
        return dto;
    }

    /**
     * Convert a protobuf Stats.EntityStats to a list of StatSnapshotApiDTO.
     *
     * EntityStats includes information about an entity as well as a list of StatsSnapshots
     * related to that entity. This list of StatsSnapshots must be converted to StatSnapshotApiDTO.
     *
     * @param entityStats the Stats.EntityStats to convert
     * @return a List of StatSnapshotApiDTO populated from the EntityStats
     */
    public static List<StatSnapshotApiDTO> toStatsSnapshotApiDtoList(EntityStats entityStats) {
        return entityStats.getStatSnapshotsList().stream()
                .map(StatsMapper::toStatSnapshotApiDTO)
                .collect(Collectors.toList());
    }

    /**
     * Convert a protobuf for a {@link StatRecord} to a {@link StatApiDTO} for return to the REST API caller (e.g. UX).
     *
     * A StatRecord contains information about a single statistic.
     *
     * @param statRecord the {@link StatRecord} protobuf to be converted to the {@link StatApiDTO} for return to the
     *                   REST API caller.
     * @return a new instance of {@link StatApiDTO} initialized from given protobuf.
     */
    private static StatApiDTO toStatApiDto(StatRecord statRecord) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(statRecord.getName());

        final BaseApiDTO provider = new BaseApiDTO();
        provider.setDisplayName(statRecord.getProviderDisplayName());
        provider.setUuid(statRecord.getProviderUuid());

        statApiDTO.setRelatedEntity(provider);

        statApiDTO.setUnits(statRecord.getUnits());
        statApiDTO.setCapacity(buildStatDTO(statRecord.getCapacity()));
        statApiDTO.setReserved(buildStatDTO(statRecord.getReserved()));

        // The "values" should be equivalent to "used".
        statApiDTO.setValues(toStatValueApiDTO(statRecord.getUsed()));
        statApiDTO.setValue(statRecord.getUsed().getAvg());

        // Build filters
        final List<StatFilterApiDTO> filters = new ArrayList<>();
        if (statRecord.hasRelation()) {
            relationFilter(statRecord.getRelation()).ifPresent(filters::add);
        }
        statApiDTO.setFilters(filters);
        return statApiDTO;
    }

    @Nonnull
    private static Optional<StatFilterApiDTO> relationFilter(@Nonnull final String relation) {
        return getUIValue(relation).map(uiRelation -> {
            final StatFilterApiDTO filter = new StatFilterApiDTO();
            filter.setType(RELATION_FILTER_TYPE);
            filter.setValue(uiRelation);
            return filter;
        });
    }

    /**
     * Convert a protobuf for a {@link StatValue} to a {@link StatValueApiDTO} for return to the REST API
     * caller (e.g. UX).
     *
     * @param statValue a {@link StatValue} protobuf to convert
     * @return a new instance of {@link StatValueApiDTO} initialized from the given protobuf
     */
    private static StatValueApiDTO toStatValueApiDTO(StatValue statValue) {
        StatValueApiDTO converted = new StatValueApiDTO();
        converted.setAvg(statValue.getAvg());
        converted.setMax(statValue.getMax());
        converted.setMin(statValue.getMin());
        converted.setTotal(statValue.getTotal());
        return converted;
    }

    /**
     * Create a {@link EntityStatsRequest} for a single UUID. Cascades into the List form.
     *
     * @param entityIds gather stats for the entities with these IDs.
     * @param statApiInput a {@link StatApiInputDTO} specifying query options for this /stats query
     * @return a new instance of {@link EntityStatsRequest} protobuf with fields set from the given statApiInput
     */
    public static EntityStatsRequest toEntityStatsRequest(final Set<Long> entityIds,
                                              @Nonnull final StatPeriodApiInputDTO statApiInput) {
        return EntityStatsRequest.newBuilder()
                .addAllEntities(entityIds)
                .setFilter(newPeriodStatsFilter(statApiInput))
                .build();
    }

    /**
     * Convert a {@link StatPeriodApiInputDTO} request from the REST API into a protobuf
     * {@link StatsFilter} to send via gRPC to the History Service.
     *
     * The StatApiInputDTO specifies the details of the /stats request, including a date range,
     * commodities to query, and one of entity names / scopes / entity-type to query.
     *
     *
     * @param statApiInput a {@link StatPeriodApiInputDTO} specifying query options for
     *                     this /stats query
     * @return a new instance of {@link StatsFilter} protobuf with fields set from the
     *        given statApiInput
     */
    @Nonnull
    private static StatsFilter newPeriodStatsFilter(
            @Nonnull final StatPeriodApiInputDTO statApiInput) {
        final StatsFilter.Builder requestBuilder = StatsFilter.newBuilder();

        final String inputStartDate = statApiInput.getStartDate();
        if (inputStartDate != null) {
            final Long aLong = Long.valueOf(inputStartDate);
            requestBuilder.setStartDate(aLong);
        }
        if (statApiInput.getEndDate() != null) {
            final Long aLong = Long.valueOf(statApiInput.getEndDate());
            requestBuilder.setEndDate(aLong);
        }
        if (statApiInput.getStatistics() != null) {
            for (StatApiInputDTO stat : statApiInput.getStatistics()) {
                requestBuilder.addCommodityName(stat.getName());

                // TODO (roman, June 2, 2017) OM-20291: Handle filters, relatedEntityType,
                // and groupBy on a per-statistic basis. Just printing warnings for now.
                if (stat.getFilters() != null && !stat.getFilters().isEmpty()) {
                    logger.warn("Unhandled statistic filters!\n {}", stat.getFilters().stream()
                        .map(filter -> filter.getType() + " : " + filter.getValue())
                        .collect(Collectors.joining("\n")));
                }
                if (stat.getRelatedEntityType() != null) {
                    logger.warn("Unhandled related entity type: {}", stat.getRelatedEntityType());
                }
                if (stat.getGroupBy() != null && !stat.getGroupBy().isEmpty()) {
                    logger.warn("Unhandled group-by for stats:\n {}",
                            stat.getGroupBy().stream().collect(Collectors.joining("\n")));
                }
            }
        }
        return requestBuilder.build();
    }

    @Nonnull
    private static StatValueApiDTO buildStatDTO(float value) {
        // TODO: This conversion is a hack. Implement properly.
        final StatValueApiDTO stat = new StatValueApiDTO();
        stat.setAvg(value);
        stat.setTotal(value);
        stat.setMin(value);
        stat.setMax(value);

        return stat;
    }

    @Nonnull
    private static Optional<String> getUIValue(@Nonnull final String dbValue) {
        final Optional<String> uiValue = dbToUiStatTypes.get(dbValue);

        if (uiValue == null) {
            throw new IllegalArgumentException("Illegal statistic type [" + dbValue + "]");
        }

        return uiValue;
    }

    /**
     * Create a request to fetch Projected Stats from the History Component.
     *
     * @param uuid a set of {@link ServiceEntityApiDTO} UUIDs to query
     * @param inputDto parameters for the query, especially the requested stats
     * @return a {@link ProjectedStatsRequest} protobuf which encapsulates the given uuid list
     * and stats names to be queried.
     */
    @Nonnull
    public static ProjectedStatsRequest toProjectedStatsRequest(
            @Nonnull final Set<Long> uuid,
            @Nonnull final StatPeriodApiInputDTO inputDto) {
        ProjectedStatsRequest.Builder builder = ProjectedStatsRequest.newBuilder().addAllEntities(uuid);
        inputDto.getStatistics().forEach(statApiInputDTO -> {
            // If necessary we can add support for other parts of the StatPeriodApiInputDTO,
            // and extend the Projected Stats API to serve the additional functionality.
            builder.addCommodityName(statApiInputDTO.getName());
        });
        return builder.build();
    }
}
