package com.vmturbo.extractor.patchers;

import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.AggregateCommodityFieldApiDTO.Aggregation;
import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.CommodityAttribute;
import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.extractor.search.EnumUtils.CommodityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.SearchEntityTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialEntityInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add aggregated commodities for groups.
 */
public class GroupAggregatedCommoditiesPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void fetch(PartialEntityInfo recordInfo, DataProvider dataProvider) {
        final List<SearchMetadataMapping> metadataList = SearchMetadataUtils.getMetadata(
                recordInfo.getGroupType(), FieldType.AGGREGATE_COMMODITY);
        final long groupId = recordInfo.getOid();

        metadataList.forEach(metadata -> {
            final int commodityType = CommodityTypeUtils.apiToProto(metadata.getCommodityType())
                    .getNumber();
            final CommodityAttribute commodityAttribute = metadata.getCommodityAttribute();
            final Aggregation commodityAggregation = metadata.getCommodityAggregation();
            Stream<Long> entityIdStream = dataProvider.getGroupLeafEntitiesOfType(groupId,
                    SearchEntityTypeUtils.apiToProto(metadata.getMemberType()));

            switch (commodityAttribute) {
                case USED:
                    getUsed(entityIdStream, commodityType, commodityAggregation, dataProvider)
                            .ifPresent(used -> recordInfo.putAttr(metadata, used));
                    break;
                case CAPACITY:
                    getCapacity(entityIdStream, commodityType, commodityAggregation, dataProvider)
                            .ifPresent(capacity -> recordInfo.putAttr(metadata, capacity));
                    break;
                case CURRENT_UTILIZATION:
                    getCurrentUtilization(groupId, entityIdStream, commodityType, commodityAggregation, dataProvider)
                            .ifPresent(utilization -> recordInfo.putAttr(metadata, utilization));
                    break;
                case WEIGHTED_HISTORICAL_UTILIZATION:
                case PERCENTILE_HISTORICAL_UTILIZATION:
                default:
                    logger.error("Unsupported group commodity attribute {}", commodityAttribute);
            }
        });
    }

    /**
     * Get aggregated used values for given commodity over given entities.
     *
     * @param entities      entities to aggregate over
     * @param commodityType the type of commodity
     * @param aggregation how to aggregate the commodity across the group
     * @param dataProvider a provider of topology-wide data
     * @return a double representing the aggregated used value, or empty
     */
    public OptionalDouble getUsed(Stream<Long> entities, int commodityType,
            Aggregation aggregation, DataProvider dataProvider) {
        DoubleStream doubleStream = entities
                .map(entity -> dataProvider.getCommodityUsed(entity, commodityType))
                .filter(OptionalDouble::isPresent)
                .mapToDouble(OptionalDouble::getAsDouble);
        switch (aggregation) {
            case AVERAGE:
                return doubleStream.average();
            case TOTAL:
                return doubleStream.reduce(Double::sum);
            default:
                logger.error("Unsupported group commodity aggregation {}", aggregation);
                return OptionalDouble.empty();
        }
    }

    /**
     * Get aggregated capacity values for given commodity over given entities.
     *
     * @param entities      entities to aggregate over
     * @param commodityType the type of commodity
     * @param aggregation how to aggregate the commodity across the group
     * @param dataProvider a provider of topology-wide data
     * @return a double representing the aggregated capacity value, or empty
     */
    public OptionalDouble getCapacity(Stream<Long> entities, int commodityType,
            Aggregation aggregation, DataProvider dataProvider) {
        DoubleStream doubleStream = entities
                .map(entity -> dataProvider.getCommodityCapacity(entity, commodityType))
                .filter(OptionalDouble::isPresent)
                .mapToDouble(OptionalDouble::getAsDouble);
        switch (aggregation) {
            case AVERAGE:
                return doubleStream.average();
            case TOTAL:
                return doubleStream.reduce(Double::sum);
            default:
                logger.error("Unsupported group commodity aggregation {}", aggregation);
                return OptionalDouble.empty();
        }
    }

    /**
     * Get the current utilization of a group, either by averaging or total.
     *
     * <p>The average utilization is the average of the utilization of all the members.
     * The total utilization is the sum of the members' commodity (e.g. used) amount divided by the
     * sum of the members' capacity.
     * Whether these two values are different largely depends on whether all members have the same
     * capacity.</p>
     *
     * @param groupId the ID of the group
     * @param entityIdStream a stream of entity IDs of group members
     * @param commodityType the type of commodity
     * @param aggregation how to aggregate the commodity across the group
     * @param dataProvider a provider of topology-wide data
     * @return a double representing the utilization as a percentage, or Optional empty
     */
    public OptionalDouble getCurrentUtilization(long groupId, Stream<Long> entityIdStream, int commodityType,
                                                Aggregation aggregation, DataProvider dataProvider) {
        switch (aggregation) {
            case AVERAGE:
                return entityIdStream.map(entity -> dataProvider.getCommodityUtilization(entity, commodityType))
                        .filter(OptionalDouble::isPresent)
                        .mapToDouble(OptionalDouble::getAsDouble)
                        .average();
            case TOTAL:
                // materialize the stream to list then use it twice
                final List<Long> entityIds = entityIdStream.collect(Collectors.toList());
                final OptionalDouble totalUsed = getUsed(entityIds.stream(), commodityType,
                        Aggregation.TOTAL, dataProvider);
                if (totalUsed.isPresent()) {
                    OptionalDouble totalCapacity = getCapacity(entityIds.stream(), commodityType,
                            Aggregation.TOTAL, dataProvider);
                    if (totalCapacity.isPresent()) {
                        double capacity = totalCapacity.getAsDouble();
                        if (capacity == 0) {
                            logger.error("Total capacity is 0 for commodity {} in group {}, "
                                            + "setting utilization to 0",
                                    CommodityType.forNumber(commodityType), groupId);
                            return OptionalDouble.of(0);
                        } else {
                            return OptionalDouble.of(totalUsed.getAsDouble() / capacity);
                        }
                    }
                }
                return OptionalDouble.empty();
            default:
                logger.error("Unsupported group commodity aggregation {}", aggregation);
                return OptionalDouble.empty();
        }
    }
}
