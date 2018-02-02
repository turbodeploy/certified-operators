package com.vmturbo.reports.component.db.converters;

import javax.annotation.Nullable;

import org.jooq.Converter;

import com.vmturbo.api.enums.ReportOutputFormat;

/**
 * Jooq converter between the DB (String) and business-logic ({@link ReportOutputFormat}).
 */
public class ReportOutputTypeConverter implements Converter<String, ReportOutputFormat> {

    @Override
    @Nullable
    public String to(ReportOutputFormat userObject) {
        return userObject.getLiteral();
    }

    @Override
    @Nullable
    public ReportOutputFormat from(String databaseObject) {
        return ReportOutputFormat.get(databaseObject);
    }

    @Override
    public Class<ReportOutputFormat> toType() {
        return ReportOutputFormat.class;
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }
}
