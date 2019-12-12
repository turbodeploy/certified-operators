package com.vmturbo.sql.utils;

import java.sql.Date;
import java.time.LocalDate;

import org.jooq.Converter;

/**
 * A converter to convert SQL dates to Java LocalDate objects automatically
 * when interacting with the DB through jOOQ.
 */
public class LocalDateConverter implements Converter<Date, LocalDate> {

    @Override
    public LocalDate from(final Date date) {
        return date == null ? null : date.toLocalDate();
    }

    @Override
    public Date to(final LocalDate localDate) {
        return localDate == null ? null : Date.valueOf(localDate);
    }

    @Override
    public Class<Date> fromType() {
        return Date.class;
    }

    @Override
    public Class<LocalDate> toType() {
        return LocalDate.class;
    }
}