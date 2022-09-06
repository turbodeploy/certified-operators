package com.vmturbo.sql.utils;

import java.sql.Timestamp;
import java.time.Instant;

import org.jooq.Converter;

/**
 * A converter to convert from DATETIME SQL data types to {@link Instant}. The DATETIME
 * data time is mapped to {@link Timestamp} in jOOQ.
 */
public class InstantDateTimeConverter implements Converter<Timestamp, Instant> {

    @Override
    public Instant from(Timestamp timestamp) {
        return timestamp.toInstant();
    }

    @Override
    public Timestamp to(Instant instant) {
        return Timestamp.from(instant);
    }

    @Override
    public Class<Timestamp> fromType() {
        return Timestamp.class;
    }

    @Override
    public Class<Instant> toType() {
        return Instant.class;
    }
}
