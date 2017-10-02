package com.vmturbo.api.component.external.api.mapper;

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
}
