package com.vmturbo.topology.processor.api.dto;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.TargetData;

/**
 * Java representation of {@link TargetData}, holding only input fields.
 */
@Immutable
public class TargetInputFields implements TargetData {
    @ApiModelProperty(value = "Account values to use to add the target.")
    private final List<InputField> inputFields;

    public TargetInputFields() {
        this.inputFields = null;
    }

    public TargetInputFields(List<? extends InputField> inputFields) {
        this.inputFields = ImmutableList.copyOf(inputFields);
    }

    public List<InputField> getInputFields() {
        return inputFields;
    }

    @Override
    public Set<AccountValue> getAccountData() {
        return new HashSet<>(inputFields);
        // TODO investigate, why spec.getInputFields() is a list instead of a set
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("input fields: ");
        sb.append(inputFields);
        return sb.toString();
    }
}