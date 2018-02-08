package com.vmturbo.reports.component.templates;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;

/**
 * Templates wrapper for on-demand templates.
 */
public class StandardTemplateWrapper implements TemplateWrapper {

    private static final String pathPrefix = "/VmtReports/";
    private final StandardReportsRecord templateRecord;

    public StandardTemplateWrapper(@Nonnull StandardReportsRecord templateRecord) {
        this.templateRecord = Objects.requireNonNull(templateRecord);
        Objects.requireNonNull(templateRecord.getFilename());
    }

    @Nonnull
    @Override
    public ReportTemplate toProtobuf() {
        final ReportTemplate.Builder builder = ReportTemplate.newBuilder();
        builder.setId(ReportTemplateId.newBuilder()
                .setId(templateRecord.getId())
                .setReportType(ReportType.BIRT_STANDARD.getValue()));
        Optional.ofNullable(templateRecord.getTitle()).ifPresent(builder::setTitle);
        Optional.ofNullable(templateRecord.getCategory()).ifPresent(builder::setCategory);
        Optional.ofNullable(templateRecord.getShortDesc()).ifPresent(builder::setShortDescription);
        builder.setDescription(templateRecord.getDescription());
        Optional.ofNullable(templateRecord.getPeriod())
                .ifPresent(period -> builder.setPeriod(period.ordinal()));
        Optional.ofNullable(templateRecord.getDayType())
                .ifPresent(dayOfWeek -> builder.setDayType(dayOfWeek.ordinal()));
        return builder.build();
    }

    @Nonnull
    @Override
    public String getTemplateFile() {
        return pathPrefix + templateRecord.getFilename() + ".rptdesign";
    }
}
