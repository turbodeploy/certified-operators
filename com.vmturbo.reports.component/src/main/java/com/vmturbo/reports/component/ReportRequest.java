package com.vmturbo.reports.component;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.ReportOutputFormat;

/**
 * Pojo, holding all the reporting parameters.
 */
@Immutable
public class ReportRequest {
    private final Map<String, String> parameters;
    private final ReportOutputFormat format;
    private final String rptDesign;

    /**
     * Constructs report request.
     *
     * @param rptDesign design id. Will be used as a path for search in classpath
     * @param format format of the report
     * @param parameters parameters to add for the report
     */
    public ReportRequest(@Nonnull String rptDesign, @Nonnull ReportOutputFormat format,
            @Nonnull Map<String, String> parameters) {
        this.rptDesign = Objects.requireNonNull(rptDesign);
        this.format = Objects.requireNonNull(format);
        this.parameters = ImmutableMap.copyOf(Objects.requireNonNull(parameters));
    }

    @Nonnull
    public String getRptDesign() {
        return rptDesign;
    }

    @Nonnull
    public ReportOutputFormat getFormat() {
        return format;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }
}
