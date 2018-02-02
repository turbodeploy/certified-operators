package com.vmturbo.reports.component.instances;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reports.component.db.tables.pojos.ReportInstance;

/**
 * Converter for report instances.
 */
@ThreadSafe
public class ReportInstanceConverter {

    private ReportInstanceConverter() {}

    /**
     * Converts DB representation of report instance into a Protobuf representation.
     *
     * @param src DB representation of report instance
     * @return Protobuf representation of the report instance
     */
    @Nonnull
    public static Reporting.ReportInstance convert(@Nonnull ReportInstance src) {
        final Reporting.ReportInstance.Builder builder = Reporting.ReportInstance.newBuilder();
        builder.setFormat(src.getOutputFormat().getLiteral());
        builder.setId(src.getId());
        builder.setGenerationTime(src.getGenerationTime().getTime());
        final ReportTemplateId templateId = ReportTemplateId.newBuilder()
                .setReportType(src.getReportType().getValue())
                .setId(src.getTemplateId())
                .build();
        builder.setTemplate(templateId);
        return builder.build();
    }
}
