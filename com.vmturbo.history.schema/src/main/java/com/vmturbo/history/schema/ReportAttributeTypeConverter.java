package com.vmturbo.history.schema;

import javax.annotation.concurrent.ThreadSafe;

import org.jooq.Converter;

import com.vmturbo.api.dto.report.ReportAttributeType;

/**
 * DB converter for report attribute type.
 */
@ThreadSafe
public class ReportAttributeTypeConverter implements Converter<String, ReportAttributeType> {
    @Override
    public ReportAttributeType from(String databaseObject) {
        return ReportAttributeType.fromDbValue(databaseObject);
    }

    @Override
    public String to(ReportAttributeType userObject) {
        return userObject == null ? null : userObject.getDbValue();
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<ReportAttributeType> toType() {
        return ReportAttributeType.class;
    }
}
