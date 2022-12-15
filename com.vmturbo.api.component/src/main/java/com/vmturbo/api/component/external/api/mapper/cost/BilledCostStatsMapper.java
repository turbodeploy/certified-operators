package com.vmturbo.api.component.external.api.mapper.cost;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TimeZone;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.mapper.stat.EntityFilterMapper;
import com.vmturbo.api.component.external.api.mapper.stat.EntityStatAttributeMapper;
import com.vmturbo.api.component.external.api.mapper.stat.EnumFilterMapper;
import com.vmturbo.api.component.external.api.mapper.stat.RelatedEntityMapper;
import com.vmturbo.api.component.external.api.mapper.stat.StatAttributeMapper;
import com.vmturbo.api.component.external.api.mapper.utils.OidExtractor;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommon.StatValues;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStat;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapper for converting {@link BilledCostStat} DTOs from the cost component to {@link StatSnapshotApiDTO}.
 */
public class BilledCostStatsMapper {

    private static final Logger logger = LogManager.getLogger();

    private static final String TAG_KEY = "tagKey";

    private static final String TAG_VALUE = "tagValue";

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    private static final List<EntityStatAttributeMapper<BilledCostStat>> ENTITY_ATTRIBUTE_MAPPERS =
            ImmutableList.<EntityStatAttributeMapper<BilledCostStat>>builder()
                    .add(RelatedEntityMapper.of(
                            OidExtractor.of(BilledCostStat::hasEntityId, BilledCostStat::getEntityId)))
                    .add(EntityFilterMapper.of(
                            OidExtractor.of(BilledCostStat::hasEntityId, BilledCostStat::getEntityId),
                            StringConstants.ENTITY))
                    .add(EntityFilterMapper.of(
                            OidExtractor.of(BilledCostStat::hasAccountId, BilledCostStat::getAccountId),
                            ApiEntityType.BUSINESS_ACCOUNT.apiStr()))
                    .add(EntityFilterMapper.of(
                            OidExtractor.of(BilledCostStat::hasRegionId, BilledCostStat::getRegionId),
                            ApiEntityType.REGION.apiStr()))
                    .add(EntityFilterMapper.of(
                            OidExtractor.of(BilledCostStat::hasCloudServiceId, BilledCostStat::getCloudServiceId),
                            ApiEntityType.CLOUD_SERVICE.apiStr()))
                    .add(EntityFilterMapper.of(
                            OidExtractor.of(BilledCostStat::hasServiceProviderId, BilledCostStat::getServiceProviderId),
                            ApiEntityType.SERVICE_PROVIDER.apiStr()))
                    .build();

    private static final List<StatAttributeMapper<BilledCostStat>> ATTRIBUTE_MAPPERS =
            ImmutableList.<StatAttributeMapper<BilledCostStat>>builder()
                    .add(EnumFilterMapper.of(BilledCostStat::hasCostCategory, BilledCostStat::getCostCategory))
                    .add(EnumFilterMapper.of(BilledCostStat::hasPriceModel, BilledCostStat::getPriceModel))
                    .add(EnumFilterMapper.of(BilledCostStat::hasEntityType, stat -> EntityType.forNumber(stat.getEntityType())))
                    .add(TagGroupMapper.create())
                    .build();


    private final UuidMapper uuidMapper;

    /**
     * Constructs a new {@link BilledCostStatsMapper} instance.
     * @param uuidMapper The UUID mapper, used to resolve OId references within {@link BilledCostStat}.
     */
    public BilledCostStatsMapper(@Nonnull UuidMapper uuidMapper) {
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
    }

    /**
     * Converts the provided {@link BilledCostStat} instances to {@link StatSnapshotApiDTO}.
     * @param billedCostStats The billed cost stats to convert.
     * @return A list of stat snapshots.
     */
    @Nonnull
    public List<StatSnapshotApiDTO> convertBilledCostStats(@Nonnull Collection<BilledCostStat> billedCostStats) {

        logger.debug("Converting {} BilledCostStat instances to StatSnapshotApiDTO", billedCostStats.size());

        final ListMultimap<Long, StatApiDTO> statsTimestampMap = ArrayListMultimap.create();

        final ListMultimap<Long, Consumer<CachedEntityInfo>> entityInfoCallbacks = ArrayListMultimap.create();
        for (BilledCostStat costStat : billedCostStats) {

            final StatApiDTO statApiDTO = new StatApiDTO();
            statsTimestampMap.put(costStat.getSampleTsUtc(), statApiDTO);

            ATTRIBUTE_MAPPERS.forEach(attributeMapper -> attributeMapper.updateStatDto(costStat, statApiDTO));
            ENTITY_ATTRIBUTE_MAPPERS.forEach(entityAttributeMapper -> {

                final OptionalLong entityOid = entityAttributeMapper.oidExtractor().resolveEntityOid(costStat);
                entityOid.ifPresent(oid -> entityInfoCallbacks.put(oid, entityInfo ->
                        entityAttributeMapper.updateStatDto(oid, entityInfo, statApiDTO)));
            });

            statApiDTO.setName(StringConstants.BILLED_COST);
            statApiDTO.setUnits(CostProtoUtil.getCurrencyUnit(costStat.getCurrency(), costStat.getGranularity()));
            statApiDTO.setValues(toStatValueDto(costStat.getCostStats()));
        }

        final Map<Long, CachedEntityInfo> entityInfoMap = uuidMapper.bulkResolveEntityInfo(entityInfoCallbacks.keys().elementSet());
        entityInfoCallbacks.asMap().forEach((entityOid, callbacks) -> {
            // may be null
            final CachedEntityInfo entityInfo = entityInfoMap.get(entityOid);
            callbacks.forEach(callback -> callback.accept(entityInfo));
        });

        return statsTimestampMap.asMap().entrySet()
                .stream()
                .map(statsGroup -> {

                    final StatSnapshotApiDTO statSnapshot = new StatSnapshotApiDTO();
                    statSnapshot.setDate(DateTimeUtil.toString(statsGroup.getKey(), UTC_TIME_ZONE));
                    statSnapshot.setEpoch(Epoch.HISTORICAL);
                    statSnapshot.setStatistics(ImmutableList.copyOf(statsGroup.getValue()));

                    return statSnapshot;
                })
                .sorted(Comparator.comparing(StatSnapshotApiDTO::getDate))
                .collect(ImmutableList.toImmutableList());
    }

    private StatValueApiDTO toStatValueDto(@Nonnull StatValues statValues) {
        final StatValueApiDTO statValueDto = new StatValueApiDTO();
        statValueDto.setMax((float)statValues.getMax());
        statValueDto.setMin((float)statValues.getMin());
        statValueDto.setAvg((float)statValues.getAvg());
        statValueDto.setTotal((float)statValues.getSum());

        return statValueDto;
    }

    /**
     * Attribute mapper for {@link com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup} to
     * a {@link com.vmturbo.api.dto.statistic.StatFilterApiDTO}.
     */
    private static class TagGroupMapper implements StatAttributeMapper<BilledCostStat> {

        static TagGroupMapper create() {
            return new TagGroupMapper();
        }

        @Override
        public void updateStatDto(@Nonnull BilledCostStat costStat, @Nonnull StatApiDTO statDto) {


            if (costStat.hasTagGroup()) {
                // This code assumes stats filters will be processed in order to correlate a tag key
                // with the tag value
                costStat.getTagGroup().getTagsMap().forEach((tagKey, tagValue) -> {

                    statDto.addFilter(TAG_KEY, tagKey);
                    statDto.addFilter(TAG_VALUE, tagValue);
                });
            }
        }
    }
}
