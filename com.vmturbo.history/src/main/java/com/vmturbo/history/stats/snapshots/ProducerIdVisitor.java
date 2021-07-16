/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.Builder;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.snapshots.ProducerIdVisitor.ProviderInformation;

/**
 * {@link ProducerIdVisitor} visits DB column which contains information about producer identifier.
 * Populates provider uuid and provider display name in results stat record.
 */
@NotThreadSafe
public class ProducerIdVisitor extends AbstractVisitor<Record, ProviderInformation> {
    /**
     * This string is the producer display name for a commodity
     * that has many providers.
     */
    public static final String MULTIPLE_PROVIDERS = "MULTIPLE PROVIDERS";

    private static final String UNKNOWN_PROVIDER = "UNKNOWN PROVIDER";

    private final boolean isFullMarket;

    private final SharedPropertyPopulator<ProviderInformation> producerIdPopulator;
    private final String propertyName;

    /**
     * Creates {@link ProducerIdVisitor} instance.
     *
     * @param isFullMarket whether we want to get stat record about full market or
     *                 not.
     * @param propertyName name of the column which contains information about
     *                 producer identifier.
     * @param producerIdPopulator populates producer identifier in {@link
     *                 StatRecord.Builder} instance.
     */
    public ProducerIdVisitor(boolean isFullMarket, @Nonnull String propertyName,
                             @Nonnull SharedPropertyPopulator<ProviderInformation> producerIdPopulator) {
        super(null);
        this.producerIdPopulator = Objects.requireNonNull(producerIdPopulator);
        this.propertyName = Objects.requireNonNull(propertyName);
        this.isFullMarket = isFullMarket;
    }

    @Override
    public void visit(@Nonnull Record record) {
        // if the new record does not contain a provider id, it can be safely ignored
        final Long rawValue = RecordVisitor.getFieldValue(record, propertyName, Long.class);
        if (rawValue == null) {
            return;
        }

        // a new record with a provider id has been found
        // ensure that there is a ProviderInformation accumulator
        // and fetch it
        final ProviderInformation state = ensureState(ProviderInformation::new, record);

        // if we are collecting provider ids (i.e., this is not a full market request),
        // then the accumulator is given the new provider id
        if (!isFullMarket) {
            state.newProviderId(rawValue);
        }
    }

    @Override
    protected void buildInternally(@Nonnull final Builder builder,
                                   @Nonnull final Record record,
                                   @Nonnull final ProviderInformation state) {
        // populate the provider id information in the record builder
        // according to the state accumulated when visiting the DB records
        producerIdPopulator.accept(builder, state, record);
    }

    /**
     * Populates values in {@link StatRecord.Builder} values which are depending on producer
     * identifier: provider UUID, provider display name.
     */
    public static class ProducerIdPopulator extends SharedPropertyPopulator<ProviderInformation> {
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
        public void accept(@Nonnull StatRecord.Builder builder,
                           @Nullable ProviderInformation providerInformation,
                           @Nullable Record record) {
            // if no provider has been recorded, then no field needs to be populated
            if (providerInformation == null || providerInformation.isEmpty()) {
                return;
            }
            // get the recorded provider id
            final Long providerId = providerInformation.getProviderId();
            if (providerId != null) {
                builder.setProviderUuid(providerId.toString());
            }
            builder.setProviderDisplayName(providerInformation.getDisplayName(liveStatsReader));
        }
    }

    /**
     * This class holds the provider uuid for a commodity stats record.
     * In the case of multiple providers or when the stat request concerns the
     * full market, no uuid is remembered.
     */
    @NotThreadSafe
    public static class ProviderInformation  implements InformationState {
        private boolean isEmpty = true;
        private Long providerId;
        private boolean isMultiple;

        /**
         * Construct empty object.
         */
        public ProviderInformation() {
            this(null);
        }

        /**
         * Construct object and possibly record one provider id.
         *
         * @param providerId a provider id to be recorded
         *                   (null, if no provider id is to be recorded)
         */
        public ProviderInformation(@Nullable Long providerId) {
            if (providerId != null) {
                newProviderId(providerId);
            }
        }

        /**
         * Record one provider id. If another provider has already been
         * recorded, then no provider information is kept.
         *
         * @param newProviderId provider id to record
         */
        public void newProviderId(Long newProviderId) {
            // Make sure isMultiple is properly updated when given records have multiple providerId.
            isMultiple = isMultiple || !isEmpty && !Objects.equals(providerId, newProviderId);
            isEmpty = false;
            providerId = newProviderId;
        }

        /**
         * Returns display name of the provider. In case there are more than one provider in the
         * collection of records then {@link ProducerIdVisitor#MULTIPLE_PROVIDERS} string will be
         * returned. In case there is no any provider identifier in records, then {@link
         * ProducerIdVisitor#UNKNOWN_PROVIDER} string will be returned. In all remaining cases
         * display name of the provider will be used retrieved from {@link LiveStatsReader}.
         *
         * @param liveStatsReader provides a display name by specified provider
         *                 identifier.
         * @return display name of the provider.
         */
        @Nonnull
        public String getDisplayName(LiveStatsReader liveStatsReader) {
            if (isMultiple) {
                return MULTIPLE_PROVIDERS;
            }
            final String displayName = liveStatsReader.getEntityDisplayNameForId(providerId);
            if (StringUtils.isBlank(displayName)) {
                return UNKNOWN_PROVIDER;
            }
            return displayName;
        }

        /**
         * Return the recorded provider id as {@link String}. Returns {@code null} when we cannot
         * identify a provider.
         *
         * @return recorded provider id or null (for multiple providers or full market)
         */
        @Nullable
        public Long getProviderId() {
            if (isMultiple) {
                return null;
            }
            return providerId;
        }

        /**
         * Returns true if and only if this object has never recorded a provider id.
         *
         * @return true if and only if this object has never recorded a provider id
         */
        @Override
        public boolean isEmpty() {
            return isEmpty;
        }

        @Override
        public boolean isMultiple() {
            return isMultiple;
        }
    }
}
