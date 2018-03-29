package com.vmturbo.reports.component.templates;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.history.schema.abstraction.tables.records.OnDemandReportsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ReportAttrsRecord;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;

/**
 * Templates wrapper for on-demand templates.
 */
public class OnDemandTemplateWrapper implements TemplateWrapper {

    private static final String PATH_PREFIX = "/VmtReportTemplates/";
    private static final String PATH_SUFFIX = ".rptdesign";
    private final OnDemandReportsRecord templateRecord;
    private final List<ReportAttrsRecord> reportAttributes;

    public OnDemandTemplateWrapper(@Nonnull OnDemandReportsRecord templateRecord,
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
                .setReportType(ReportType.BIRT_ON_DEMAND.getValue()));
        builder.setIsGroupScoped("Group".equals(templateRecord.getScopeType()));
        Optional.ofNullable(templateRecord.getTitle()).ifPresent(builder::setTitle);
        Optional.ofNullable(templateRecord.getCategory()).ifPresent(builder::setCategory);
        Optional.ofNullable(templateRecord.getShortDesc()).ifPresent(builder::setShortDescription);
        builder.setDescription(templateRecord.getDescription());
        reportAttributes.stream().map(AttributeConverter::convert).forEach(builder::addAttributes);
        return builder.build();
    }

    @Nonnull
    @Override
    public String getTemplateFile() {
        return PATH_PREFIX + templateRecord.getFilename() + PATH_SUFFIX;
    }
}
