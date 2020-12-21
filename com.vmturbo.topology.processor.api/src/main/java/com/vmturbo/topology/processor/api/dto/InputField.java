package com.vmturbo.topology.processor.api.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;

/**
 * Java representation of the {@link com.vmturbo.platform.common.dto.Discovery.AccountValue}
 * proto. Also, used for integration with GSON/JSON and Swagger.
 */
@Immutable
public class InputField implements AccountValue {
    private final String name;
    private final String value;
    private final List<List<String>> groupProperties;

    protected InputField() {
        name = null;
        value = null;
        groupProperties = null;
    }

    public InputField(@Nonnull final String name, @Nonnull final String value,
                    final Optional<List<List<String>>> groupProperties) {
        this.name = name;
        this.value = value;
        if (groupProperties.isPresent()) {
            this.groupProperties = createUnmodifiable(groupProperties.get());
        } else {
            this.groupProperties = Collections.emptyList();
        }
    }

    public InputField(@Nonnull final TopologyProcessorDTO.AccountValue accountValue) {
        this.name = accountValue.getKey();
        this.value = accountValue.getStringValue();
        this.groupProperties = Collections.unmodifiableList(accountValue
                        .getGroupScopePropertyValuesList().stream()
                        .map(propList -> propList.getValueList()).collect(Collectors.toList()));
    }

    private List<List<String>> createUnmodifiable(List<List<String>> src) {
        final List<List<String>> outerList = new ArrayList<>(src.size());
        src.forEach(list -> outerList.add(Collections.unmodifiableList(list)));
        return Collections.unmodifiableList(outerList);
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public TopologyProcessorDTO.AccountValue toAccountValue() {
        Objects.requireNonNull(name, () -> "Key is not set for input value " + this);
        if (value == null && groupProperties == null) {
            throw new NullPointerException(
                            "String value or group scope should be set for input value " + this);
        }
        final TopologyProcessorDTO.AccountValue.Builder builder =
                        TopologyProcessorDTO.AccountValue.newBuilder().setKey(name);
        if (value != null) {
            builder.setStringValue(value);
        }
        if (groupProperties != null) {
            builder.addAllGroupScopePropertyValues(groupProperties.stream()
                            .map(propList -> TopologyProcessorDTO.AccountValue.PropertyValueList
                                            .newBuilder().addAllValue(propList).build())
                            .collect(Collectors.toList()));
        }
        return builder.build();
    }

    public List<List<String>> getGroupProperties() {
        return groupProperties;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append(":").append(name).append(":");
        if (getGroupScopeProperties().isEmpty()) {
            sb.append(value);
        } else {
            sb.append(getGroupScopeProperties());
        }
        return sb.toString();
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
        return Objects.hash(getName(), getValue(), getGroupScopeProperties());
    }

    @Override
    public String getStringValue() {
        return value;
    }

    @Override
    public List<List<String>> getGroupScopeProperties() {
        return groupProperties;
    }
}
