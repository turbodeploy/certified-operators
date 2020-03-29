/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * {@link PropertyTypeVisitor} visits property type field in the DB record and populates name, units
 * and stat key fields in {@link StatRecord.Builder}.
 *
 * @param <D> type of the value read form database.
 */
@ThreadSafe
public class PropertyTypeVisitor<D> extends BasePropertyVisitor<Record, Pair<String, String>, D> {

    /**
     * Creates {@link PropertyTypeVisitor} instance.
     *
     * @param databaseType type of the value read form database.
     * @param propertyName name of the column which contains information about
     *                 property type.
     * @param propertyTypeToString function to convert property type into string
     *                 representation.
     * @param propertyTypePopulator populates property type in {@link
     *                 StatRecord.Builder} instance.
     * @param fullMarket whether we want to get stat record about full market or
     *                 not.
     */
    public PropertyTypeVisitor(boolean fullMarket, @Nonnull String propertyName,
                    @Nonnull Class<D> databaseType,
                    @Nonnull Function<D, String> propertyTypeToString,
                    @Nonnull SharedPropertyPopulator<Pair<String, String>> propertyTypePopulator) {
        super(propertyName, (record, value) -> {
            // In the full-market request we return aggregate stats with no commodity_key.
            final String commodityKey = fullMarket ?
                            null :
                            RecordVisitor.getFieldValue(record, StringConstants.COMMODITY_KEY,
                                            String.class);
            return new Pair<>(propertyTypeToString.apply(value), commodityKey);
        }, propertyTypePopulator, databaseType);
    }

    /**
     * Populates name, units and stat key fields in {@link StatRecord.Builder}.
     */
    public static class PropertyTypePopulator
                    extends SharedPropertyPopulator<Pair<String, String>> {
        private static final String[] FLOW_NAMES =
                        new String[] {"InProvider", "InDPOD", "CrossDPOD", "CrossSite"};

        private static String getPropertyType(String value, String commodityKey) {
            if (value.contains("Flow") && commodityKey != null && commodityKey.length() > 5) {
                final int index = Integer.parseInt(commodityKey.substring(5));
                return FLOW_NAMES[index];
            }
            return value;
        }

        @Override
        public void accept(@Nonnull StatRecord.Builder builder,
                        Pair<String, String> propertyTypeCommodityKey, Record record) {
            final String propertyType = getPropertyType(propertyTypeCommodityKey.first,
                            propertyTypeCommodityKey.second);
            if (StringUtils.isNotBlank(propertyType)) {
                if (whetherToSet(builder.hasName(), record)) {
                    builder.setName(propertyType);
                }
                final CommodityTypeUnits commodityTypeUnits = getCommodityTypeUnits(propertyType);
                if (commodityTypeUnits != null && (whetherToSet(builder.hasUnits(), record))) {
                    builder.setUnits(commodityTypeUnits.getUnits());
                }
            }
            if (propertyTypeCommodityKey.second != null && whetherToSet(builder.hasStatKey(),
                            record)) {
                builder.setStatKey(propertyTypeCommodityKey.second);
            }
        }

        @Nullable
        private static CommodityTypeUnits getCommodityTypeUnits(@Nonnull String propertyType) {
            final CommodityTypeUnits commodityTypeUnits =
                            CommodityTypeUnits.fromString(propertyType);
            if (commodityTypeUnits != null) {
                return commodityTypeUnits;
            }
            if (propertyType.startsWith(StringConstants.STAT_PREFIX_CURRENT)) {

                //Plan aggregated source stats have "current" prefix attached
                //We need to remove this prefix and do case insensitive match for CommodityTypeUnits
                //No matches occurs for metrics, {@link StatsMapper.METRIC_NAMES}, i.e numVMs

                final String removedCurrentPrefix = propertyType.substring(
                                StringConstants.STAT_PREFIX_CURRENT.length());
                return CommodityTypeUnits.fromStringIgnoreCase(removedCurrentPrefix);
            }
            return null;
        }
    }
}
