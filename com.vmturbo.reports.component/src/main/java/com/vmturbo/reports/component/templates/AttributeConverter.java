package com.vmturbo.reports.component.templates;

import javax.annotation.Nonnull;

import com.vmturbo.history.schema.abstraction.tables.records.ReportAttrsRecord;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportAttribute;

/**
 * Converter for report attributes.
 */
public class AttributeConverter {

    private AttributeConverter() {}

    /**
     * Converts DB representation of report attribute in to a Protobuf one.
     * Inspite of report attribute's type is NULLABLE in the database, we throw nere an NPE, as
     * logically this value is required.
     *
     * @param reportAttrs DB representation
     * @return Protobuf representation
     */
    @Nonnull
    public static ReportAttribute convert(@Nonnull ReportAttrsRecord reportAttrs) {
        return ReportAttribute.newBuilder()
                .setName(reportAttrs.getName())
                .setDefaultValue(reportAttrs.getDefaultValue())
                .setValueType(reportAttrs.getAttType().name())
                .build();
    }
}
