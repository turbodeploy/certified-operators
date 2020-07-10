package com.vmturbo.cloud.commitment.analysis.topology;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.group.api.GroupMemberRetriever;

/**
 * A factory interface for building {@link BillingFamilyRetriever} instances.
 */
public interface BillingFamilyRetrieverFactory {

    /**
     * Constructs a new instance of {@link BillingFamilyRetriever}.
     * @return The newly created instance of {@link BillingFamilyRetriever}.
     */
    @Nonnull
    BillingFamilyRetriever newInstance();

    /**
     * A default implementation of a {@link BillingFamilyRetrieverFactory}.
     */
    class DefaultBillingFamilyRetrieverFactory implements BillingFamilyRetrieverFactory {

        private final GroupMemberRetriever groupMemberRetriever;

        /**
         * Constructs a new factory.
         * @param groupMemberRetriever The {@link GroupMemberRetriever}, used for querying available
         *                             billing family groups.
         */
        public DefaultBillingFamilyRetrieverFactory(@Nonnull GroupMemberRetriever groupMemberRetriever) {
            this.groupMemberRetriever = Objects.requireNonNull(groupMemberRetriever);
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public BillingFamilyRetriever newInstance() {
            return new BillingFamilyRetrieverImpl(groupMemberRetriever);
        }
    }
}
