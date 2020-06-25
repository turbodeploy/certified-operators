package com.vmturbo.extractor.search;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.AggregateCommodityFieldApiDTO.Aggregation;
import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.CommodityAttribute;
import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add aggregated commodities for groups.
 */
public class GroupAggregatedCommoditiesPatcher implements EntityRecordPatcher<DataProvider> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, DataProvider dataProvider) {
        final List<SearchMetadataMapping> metadataList = SearchMetadataUtils.getMetadata(
                recordInfo.groupType, FieldType.AGGREGATE_COMMODITY);
        final long groupId = recordInfo.oid;
        final Map<String, Object> attrs = recordInfo.attrs;

        metadataList.forEach(metadata -> {
            final String jsonKey = metadata.getJsonKeyName();
            final int commodityType = EnumUtils.commodityTypeFromApiToProtoInt(metadata.getCommodityType());
            final CommodityAttribute commodityAttribute = metadata.getCommodityAttribute();
            final Aggregation commodityAggregation = metadata.getCommodityAggregation();
            Stream<Long> entities = dataProvider.getGroupLeafEntitiesOfType(groupId,
                    EnumUtils.entityTypeFromApiToProto(metadata.getMemberType()));

            switch (commodityAttribute) {
                case USED:
                    getUsed(entities, commodityType, commodityAggregation, dataProvider)
                            .ifPresent(used -> attrs.put(jsonKey, used));
                    break;
                case CAPACITY:
                    getCapacity(entities, commodityType, commodityAggregation, dataProvider)
                            .ifPresent(capacity -> attrs.put(jsonKey, capacity));
                    break;
                case UTILIZATION:
                    getUtilization(groupId, entities, commodityType, commodityAggregation, dataProvider)
                            .ifPresent(utilization -> attrs.put(jsonKey, utilization));
                    break;
                default:
                    logger.error("Unsupported group commodity attribute {}", commodityAttribute);
            }
        });
    }

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

    public OptionalDouble getUtilization(long groupId, Stream<Long> entities, int commodityType,
            Aggregation aggregation, DataProvider dataProvider) {
        switch (aggregation) {
            case AVERAGE:
                return entities.map(entity -> dataProvider.getCommodityUtilization(entity, commodityType))
                        .filter(OptionalDouble::isPresent)
                        .mapToDouble(OptionalDouble::getAsDouble)
                        .average();
            case TOTAL:
                // materialize the stream to list then use it twice
                final List<Long> entityIds = entities.collect(Collectors.toList());
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
