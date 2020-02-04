/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.history.stats.readers.LiveStatsReader;

/**
 * {@link ProducerIdVisitor} visits DB column which contains information about producer identifier.
 * Populates provider uuid and provider display name in results stat record.
 */
@ThreadSafe
public class ProducerIdVisitor extends BasePropertyVisitor<Record, Long, Object> {
    private static final Logger LOGGER = LogManager.getLogger(ProducerIdVisitor.class);
    private static final Map<Class<?>, Function<?, Long>> DB_TYPE_TO_LONG_CONVERTER =
                    ImmutableMap.of(Long.class, Function.identity(), String.class,
                                    (String dbValue) -> {
                                        if (!NumberUtils.isCreatable(dbValue)) {
                                            LOGGER.warn("Cannot parse producer OID from '{}' value",
                                                            dbValue);
                                            return null;
                                        }
                                        return Long.valueOf(dbValue);
                                    });

    /**
     * Creates {@link ProducerIdVisitor} instance.
     *
     * @param fullMarket whether we want to get stat record about full market or
     *                 not.
     * @param propertyName name of the column which contains information about
     *                 producer identifier.
     * @param producerIdPopulator populates producer identifier in {@link
     *                 StatRecord.Builder} instance.
     */
    public ProducerIdVisitor(boolean fullMarket, @Nonnull String propertyName,
                    @Nonnull SharedPropertyPopulator<Long> producerIdPopulator) {
        super(propertyName, (record, rawProducerId) -> {
            // In the full-market request we return aggregate stats with no producer_uuid.
            if (fullMarket) {
                return null;
            }
            return castToLong(rawProducerId);
        }, producerIdPopulator, Object.class);
    }

    @Nullable
    private static <T> Long castToLong(@Nullable T rawProducerId) {
        if (rawProducerId == null) {
            return null;
        }
        final Class<?> producerIdType = rawProducerId.getClass();
        @SuppressWarnings("unchecked")
        final Function<T, Long> producerIdParser =
                        (Function<T, Long>)DB_TYPE_TO_LONG_CONVERTER.get(producerIdType);
        if (producerIdParser == null) {
            LOGGER.error("Cannot find parser for '{}' producer identifier, which has '{}' database type",
                            rawProducerId, producerIdType.getSimpleName());
            return null;
        }
        return producerIdParser.apply(rawProducerId);
    }

    /**
     * Populates values in {@link StatRecord.Builder} values which are depending on producer
     * identifier: provider UUID, provider display name.
     */
    public static class ProducerIdPopulator extends SharedPropertyPopulator<Long> {
        private final LiveStatsReader liveStatsReader;

        /**
         * Creates {@link ProducerIdPopulator} instance.
         *
         * @param liveStatsReader provides information about live statistics.
         */
        public ProducerIdPopulator(@Nonnull LiveStatsReader liveStatsReader) {
            this.liveStatsReader = liveStatsReader;
        }

        @Override
        public void accept(@Nonnull StatRecord.Builder builder, @Nullable Long producerId,
                        @Nullable Record record) {
            final String producerIdString = String.valueOf(producerId);
            if (whetherToSet(builder.hasProviderUuid(), record)) {
                builder.setProviderUuid(producerIdString);
            }
            final String displayName = liveStatsReader.getEntityDisplayNameForId(producerId);
            if (StringUtils.isNotBlank(displayName) && whetherToSet(
                            builder.hasProviderDisplayName(), record)) {
                builder.setProviderDisplayName(displayName);
            }
        }
    }
}
