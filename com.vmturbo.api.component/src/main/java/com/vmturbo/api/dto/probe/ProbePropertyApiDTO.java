package com.vmturbo.api.dto.probe;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description="Model to describe a probe property")
@JsonInclude(Include.NON_NULL)
public class ProbePropertyApiDTO {
    @ApiModelProperty("Probe id.")
    private long probeId = 0L;

    @ApiModelProperty("Target id.")
    private Long targetId;

    @ApiModelProperty(value = "Probe property name.", required = true)
    private String name = "";

    @ApiModelProperty(value = "Probe property value.", required = true)
    private String value = "";

    public long getProbeId() {
        return probeId;
    }

    public Long getTargetId() {
        return targetId;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public String getValue() {
        return value;
    }

    public void setProbeId(long probeId) {
        this.probeId = probeId;
    }

    public void setTargetId(long targetId) {
        this.targetId = targetId;
    }

    public void setName(@Nonnull String name) {
        this.name = name;
    }

    public void setValue(@Nonnull String value) {
        this.value = value;
    }
}
