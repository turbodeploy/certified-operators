package com.vmturbo.topology.processor.api.dto;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.TargetData;

/**
 * Java representation of {@link TargetData}, holding input fields and label.
 */
@Immutable
public class TargetInputFields implements TargetData {
    @ApiModelProperty(value = "Account values to use to add the target.")
    private final List<InputField> inputFields;

    @ApiModelProperty(value = "The communication channel of the target")
    private String communicationBindingChannel = null;

    public TargetInputFields() {
        this.inputFields = null;
    }

    /**
    * Creates a new instance of {@link TargetInputFields}.
    *
    * @param inputFields values
    * @param communicationBindingChannel optional of the channel
    **/
    public TargetInputFields(List<? extends InputField> inputFields, Optional<String> communicationBindingChannel) {
        this.inputFields = ImmutableList.copyOf(inputFields);
        this.communicationBindingChannel = communicationBindingChannel.orElse(null);
    }

    public List<InputField> getInputFields() {
        return inputFields;
    }

    @Override
    public Set<AccountValue> getAccountData() {
        return new HashSet<>(inputFields);
        // TODO investigate, why spec.getInputFields() is a list instead of a set
    }

    @Override
    @Nonnull
    public Optional<String> getCommunicationBindingChannel() {
        return Optional.ofNullable(communicationBindingChannel);
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("input fields: ");
        sb.append(inputFields);
        return sb.toString();
    }
}
