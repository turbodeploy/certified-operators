package com.vmturbo.reports.component.db.converters;

import javax.annotation.Nullable;

import org.jooq.Converter;

import com.vmturbo.api.enums.ReportType;

/**
 * DB (Jooq) converter for report type enumeration.
 */
public class ReportTypeConverter implements Converter<Short, ReportType> {

    @Override
    @Nullable
    public ReportType from(Short databaseObject) {
        if (databaseObject == null) {
            return null;
        } else {
            return ReportType.get(databaseObject.intValue());
        }
    }

    @Override
    @Nullable
    public Short to(ReportType userObject) {
        return userObject == null ? null : (short)userObject.getValue();
    }

    @Override
    public Class<Short> fromType() {
        return Short.class;
    }

    @Override
    public Class<ReportType> toType() {
        return ReportType.class;
    }
}
