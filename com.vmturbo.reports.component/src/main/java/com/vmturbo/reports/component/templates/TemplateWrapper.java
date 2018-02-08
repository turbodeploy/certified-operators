package com.vmturbo.reports.component.templates;

import javax.annotation.Nonnull;

import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;

/**
 * Converter between DB representation and protobuf representation of reporting templates.
 */
public interface TemplateWrapper {

    /**
     * Converts report template from DB representation into protobuf.
     *
     * @return protobuf representation
     */
    @Nonnull
    ReportTemplate toProtobuf();

    /**
     * Returns the path to template filename, used for generation.
     * @return
     */
    @Nonnull
    String getTemplateFile();
}
