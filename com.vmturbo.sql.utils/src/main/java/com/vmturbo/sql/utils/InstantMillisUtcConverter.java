package com.vmturbo.sql.utils;

import java.time.Instant;

import org.jooq.Converter;

/**
 * Converter between BIGINT (long) database values representing milliseconds since epoch
 * in UTC and {@link Instant}.
 */
public class InstantMillisUtcConverter implements Converter<Long, Instant> {

    @Override
    public Instant from(Long millis) {
        return Instant.ofEpochMilli(millis);
    }

    @Override
    public Long to(Instant instant) {
        return  instant.toEpochMilli();
    }

    @Override
    public Class<Long> fromType() {
        return Long.class;
    }

    @Override
    public Class<Instant> toType() {
        return Instant.class;
    }
}
