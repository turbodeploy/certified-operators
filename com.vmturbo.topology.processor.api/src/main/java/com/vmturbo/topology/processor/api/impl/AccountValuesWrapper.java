package com.vmturbo.topology.processor.api.impl;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;

/**
 * Wrapper for protobuf {@link TopologyProcessorDTO.AccountValue}.
 */
public class AccountValuesWrapper implements AccountValue {
    private final TopologyProcessorDTO.AccountValue src;

    public AccountValuesWrapper(TopologyProcessorDTO.AccountValue src) {
        this.src = src;
    }

    @Override
    public String getName() {
        return src.getKey();
    }

    @Override
    public String getStringValue() {
        return src.getStringValue();
    }

    @Override
    public List<List<String>> getGroupScopeProperties() {
        return src.getGroupScopePropertyValuesList().stream().map(m -> m.getValueList())
                        .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return getName() + ":" + getStringValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AccountValue)) {
            return false;
        }
        final AccountValue other = (AccountValue)obj;
        return Objects.equals(getName(), other.getName())
                        && Objects.equals(getStringValue(), other.getStringValue())
                        && Objects.equals(getGroupScopeProperties(),
                                        other.getGroupScopeProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getStringValue(), getGroupScopeProperties());
    }

}
