package com.vmturbo.reports.component.templates;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;

/**
 * Converter between DB representation and protobuf representation of reporting templates.
 */
public class TemplateConverter {

    private TemplateConverter() {}

    /**
     * Converts report template from DB representation into protobuf.
     *
     * @param src source data from the DB
     * @return protobuf representation
     */
    @Nonnull
    public static ReportTemplate convert(@Nonnull StandardReportsRecord src) {
        final ReportTemplate.Builder builder = ReportTemplate.newBuilder();
        builder.setId(src.getId());
        Optional.ofNullable(src.getFilename()).ifPresent(builder::setFilename);
        Optional.ofNullable(src.getTitle()).ifPresent(builder::setTitle);
        Optional.ofNullable(src.getCategory()).ifPresent(builder::setCategory);
        Optional.ofNullable(src.getShortDesc()).ifPresent(builder::setShortDescription);
        builder.setDescription(src.getDescription());
        Optional.ofNullable(src.getPeriod())
                .ifPresent(period -> builder.setPeriod(period.ordinal()));
        Optional.ofNullable(src.getDayType())
                .ifPresent(dayOfWeek -> builder.setDayType(dayOfWeek.ordinal()));
        builder.setReportType(ReportType.BIRT_STANDARD.getValue());
        return builder.build();
    }
}
