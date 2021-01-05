/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.Builder;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.history.stats.snapshots.PropertyTypeVisitor.PropertyInformation;

/**
 * {@link PropertyTypeVisitor} visits property type field in the DB record and populates name, units
 * and stat key fields in {@link StatRecord.Builder}.
 *
 * @param <D> type of the value read form database.
 */
@NotThreadSafe
public class PropertyTypeVisitor<D> extends AbstractVisitor<Record, PropertyInformation> {
    /**
     * This string is the commodity key name for a commodity
     * that has many keys.
     */
    public static final String MULTIPLE_KEYS = "MULTIPLE KEYS";

    private final boolean isFullMarket;
    private final String propertyName;
    private final Class<D> databaseType;
    private final Function<D, String> propertyTypeToString;
    private final SharedPropertyPopulator<PropertyInformation> propertyTypePopulator;

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
                    @Nonnull SharedPropertyPopulator<PropertyInformation> propertyTypePopulator) {
        super(null);
        this.isFullMarket = fullMarket;
        this.propertyName = propertyName;
        this.databaseType = databaseType;
        this.propertyTypeToString = propertyTypeToString;
        this.propertyTypePopulator = propertyTypePopulator;
    }

    @Override
    public void visit(@Nonnull Record record) {
        /*
        * If the new record does not contain a property type it can be ignored
        * aggregating over null property types is a non valid case
        */
        final D rawValue = RecordVisitor.getFieldValue(record, propertyName, databaseType);
        if (rawValue == null) {
            return;
        }

        /*
         * A new record has been found
         * ensure that there is a ProviderInformation accumulator
         * and fetch it.
         */
        final PropertyInformation state = ensureState(PropertyInformation::new, record);

        // the accumulator is given the property type and an optional key
        final String key = isFullMarket
                                ? null
                                : RecordVisitor.getFieldValue(
                                        record, StringConstants.COMMODITY_KEY, String.class);
        state.newRecord(propertyTypeToString.apply(rawValue), key);
    }

    @Override
    protected void buildInternally(@Nonnull final Builder builder,
                                   @Nonnull final Record record,
                                   @Nonnull final PropertyInformation state) {
        /*
         * Populate the provider id information in the record builder
         * according to the state accumulated when visiting the DB records.
        */
         propertyTypePopulator.accept(builder, state, record);
    }

    /**
     * Populates name, units and stat key fields in {@link StatRecord.Builder}.
     */
    public static class PropertyTypePopulator
                    extends SharedPropertyPopulator<PropertyInformation> {

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
                           PropertyInformation propertyInformation,
                           Record record) {
            if (propertyInformation == null || propertyInformation.isEmpty()) {
                return;
            }
            final String propertyType = propertyInformation.isMultiple()
                                            ? propertyInformation.getPropertyType()
                                            : getPropertyType(propertyInformation.getPropertyType(),
                                                              propertyInformation.getKey());
            if (StringUtils.isNotBlank(propertyType)) {
                if (whetherToSet(builder.hasName(), record)) {
                    builder.setName(propertyType);
                }
                final CommodityTypeUnits commodityTypeUnits = getCommodityTypeUnits(propertyType);
                if (commodityTypeUnits != null && (whetherToSet(builder.hasUnits(), record))) {
                    builder.setUnits(commodityTypeUnits.getUnits());
                }
            }
            final String key = propertyInformation.isMultiple()
                                    ? MULTIPLE_KEYS
                                    : propertyInformation.getKey();
            if (StringUtils.isNotBlank(key) && whetherToSet(builder.hasStatKey(), record)) {
                builder.setStatKey(key);
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

                /* Plan aggregated source stats have "current" prefix attached
                 * We need to remove this prefix and do case insensitive match for CommodityTypeUnits
                 * No matches occurs for metrics, {@link StatsMapper.METRIC_NAMES}, i.e numVMs
                 */
                final String removedCurrentPrefix = propertyType.substring(
                                StringConstants.STAT_PREFIX_CURRENT.length());
                return CommodityTypeUnits.fromStringIgnoreCase(removedCurrentPrefix);
            }
            return null;
        }
    }

    /**
     * This class holds property name and key for a commodity stats record.
     * In the case of multiple keys or when the stat request concerns the
     * full market, the key is not remembered.
     */
    @NotThreadSafe
    public static class PropertyInformation implements InformationState {
        private static final Logger LOGGER = LogManager.getLogger(PropertyInformation.class);

        private boolean isEmpty = true;
        private boolean multipleKeys = false;
        private String propertyType;
        private String key;

        /**
         * Create an emtpy {@link PropertyInformation} object.
         */
        public PropertyInformation() {
        }

        /**
         * Create a {@link PropertyInformation} object and pass it a property type
         * and (possibly) a key.
         *
         * @param propertyType property type
         * @param key optional commodity key
         */
        public PropertyInformation(@Nonnull String propertyType, @Nullable String key) {
            newRecord(propertyType, key);
        }

        /**
         * Record one commodity name and a key.
         *
         * @param propertyType property type
         * @param key key
         */
        public void newRecord(@Nonnull String propertyType, @Nullable String key) {
            if (isEmpty) {
                this.propertyType = propertyType;
                this.key = key;
                isEmpty = false;
                return;
            }

            if (!StringUtils.equals(propertyType, this.propertyType)) {
                LOGGER.warn("Aggregation of stats with different commodity names '{}','{}'", propertyType, this.propertyType);
            }

            /*
             * Check if there are multiple keys
             */
            if (!isMultiple() && !StringUtils.equals(key, this.key)) {
                multipleKeys = true;
                this.key = null;
            }

        }

        /**
         * Return the recorded property type.
         *
         * @return recorded property type or null (for full market)
         */
        @Nullable
        public String getPropertyType() {
            return propertyType;
        }

        /**
         * Return the recorded key.
         *
         * @return recorded property type or null (for full market or multiple keys)
         */
        @Nullable
        public String getKey() {
            return key;
        }

        /**
         * Returns true if and only if this object has never recorded a property type.
         *
         * @return true if and only if this object has never recorded a property type
         */
        @Override
        public boolean isEmpty() {
            return isEmpty;
        }

        /**
         * Returns true if and only if this object has encountered multiple keys.
         *
         * @return true if and only if this object has encountered multiple keys
         */
        @Override
        public boolean isMultiple() {
            return multipleKeys;
        }
    }
}
