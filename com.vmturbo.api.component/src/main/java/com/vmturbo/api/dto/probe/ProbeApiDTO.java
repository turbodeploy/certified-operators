package com.vmturbo.api.dto.probe;

import java.io.Serializable;

import javax.annotation.Nonnull;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.api.dto.BaseApiDTO;

@ApiModel(description = "Model to describe a probe.")
public class ProbeApiDTO extends BaseApiDTO implements Serializable {
    @ApiModelProperty(value = "Probe id.", required = true)
    private long id;

    @ApiModelProperty(value = "Probe type.", required = true)
    private String type;

    @ApiModelProperty(value = "Probe category.", required = true)
    private String category;

    @ApiModelProperty("Unexpected error while collecting probe information")
    private String error;

    public long getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getCategory() {
        return category;
    }

    public String getError() {
        return error;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setType(@Nonnull String name) {
        this.type = name;
    }

    public void setCategory(@Nonnull String category) {
        this.category = category;
    }

    public void setError(@Nonnull String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return "{Probe: " + type + ".  ID: " + id + ".  Category:" + category + "}";
    }
}
