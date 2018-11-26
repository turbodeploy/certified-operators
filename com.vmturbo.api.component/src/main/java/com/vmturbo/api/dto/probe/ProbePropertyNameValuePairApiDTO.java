package com.vmturbo.api.dto.probe;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description="Model to describe a name/value pair for probe properties.")
public class ProbePropertyNameValuePairApiDTO {
    @ApiModelProperty(value = "Probe property name.", required = true)
    private String name = null;

    @ApiModelProperty(value = "Probe property value.", required = true)
    private String value = null;

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
