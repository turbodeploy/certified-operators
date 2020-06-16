package com.vmturbo.sql.utils;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.jooq.Converter;

/**
 * A converter to convert SQL timestamps to Java LocalDateTime objects automatically
 * when interacting with the DB through jOOQ
 */
public class LocalDateTimeConverter implements Converter<Timestamp, LocalDateTime> {
	private static final long serialVersionUID = -5105006319115651197L;

    @Override
    public LocalDateTime from(Timestamp t) {
        return t == null ? null : t.toLocalDateTime();
    }

    @Override
    public Timestamp to(LocalDateTime u) {
        return u == null ? null : Timestamp.valueOf(u);
    }

    @Override
    public Class<Timestamp> fromType() {
        return Timestamp.class;
    }

    @Override
    public Class<LocalDateTime> toType() {
        return LocalDateTime.class;
    }
}