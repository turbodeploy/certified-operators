package com.vmturbo.api.component.external.api.mapper;

import java.util.Set;

import org.springframework.util.CollectionUtils;

/**
 * Mapper to convert string UUID's to OID's that make sense in the
 * XL system. This class, in addition to {@link ApiId}, should encapsulate
 * all the weird constants, corner-cases, and magic strings involved in dealing
 * with the UI requests.
 */
public class UuidMapper {

    /**
     * In the UI, the "Market" identifies the real-time topology.
     */
    public static final String UI_REAL_TIME_MARKET_STR = "Market";

    private final long realtimeContextId;

    public UuidMapper(final long realtimeContextId) {
        this.realtimeContextId = realtimeContextId;
    }

    public ApiId fromUuid(String uuid) {
        final boolean isRealtime = uuid.equals(UI_REAL_TIME_MARKET_STR);
        return new ApiId(isRealtime ? realtimeContextId : Long.valueOf(uuid), uuid);
    }

    /**
     * A class to represent an id for interactions between the external API and XL.
     */
    public static class ApiId {
        private final long oid;
        private final String uuid;

        private ApiId(final long value, final String uuid) {
            this.oid = value;
            this.uuid = uuid;
        }

        public long oid() {
            return oid;
        }

        public String uuid() {
            return uuid;
        }

        public boolean isRealtimeMarket() {
            return uuid.equals(UI_REAL_TIME_MARKET_STR);
        }
    }

    public static boolean isRealtimeMarket(String uuid) {
        return uuid.equals(UI_REAL_TIME_MARKET_STR);
    }

    /**
     * Detect whether this is a global or scoped UUID list. If there are any seed UUIDs,
     * and none of those seeds are "Market", then this is a limited scope.
     * In other words, if there are no seeds, or any of the seeds are "Market", then this is
     * _not_ a limited scope.
     *
     * @param seedUuids the set of seedUuids to define the scope
     * @return true iff there are either more than one seed uuids, or a single seed UUID
     * that is not equal to the distinguished live market UUID "Market"
     */
    public static boolean hasLimitedScope(Set<String> seedUuids) {
        return !CollectionUtils.isEmpty(seedUuids) && !seedUuids.contains(UI_REAL_TIME_MARKET_STR);
    }
}
