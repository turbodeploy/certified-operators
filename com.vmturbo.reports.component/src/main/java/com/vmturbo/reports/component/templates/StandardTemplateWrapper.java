package com.vmturbo.reports.component.templates;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.history.schema.abstraction.tables.records.ReportAttrsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.StandardReportsRecord;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;

/**
 * Templates wrapper for on-demand templates.
 */
public class StandardTemplateWrapper implements TemplateWrapper {

    private static final String PATH_PREFIX = "/VmtReports/";
    private static final String PATH_SUFFIX = ".rptdesign";
    private final StandardReportsRecord templateRecord;
    private final List<ReportAttrsRecord> reportAttributes;

    public StandardTemplateWrapper(@Nonnull StandardReportsRecord templateRecord,
            @Nonnull List<ReportAttrsRecord> reportAttributes) {
        this.templateRecord = Objects.requireNonNull(templateRecord);
        this.reportAttributes = Objects.requireNonNull(reportAttributes);
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
        reportAttributes.stream().map(AttributeConverter::convert).forEach(builder::addAttributes);
        return builder.build();
    }

    @Nonnull
    @Override
    public String getTemplateFile() {
        return PATH_PREFIX + templateRecord.getFilename() + PATH_SUFFIX;
    }
}
