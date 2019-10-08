package com.vmturbo.components.common.setting;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Data structure, that is declared for this setting.
 *
 * @param <T> type of the internal data stored.
 */
@Immutable
public abstract class AbstractSettingDataType<T> implements SettingDataStructure<T> {
    /**
     * Default value for the setting.
     */
    private final T defaultValue;
    /**
     * Entity-specific overrides for default values.
     */
    private final Map<EntityType, T> entityDefaults;

    protected AbstractSettingDataType(@Nonnull T defaultValue,
                                      @Nonnull Map<EntityType, T> entityDefaults) {
        this.defaultValue = Objects.requireNonNull(defaultValue);
        this.entityDefaults = entityDefaults.isEmpty() ? Collections.emptyMap() :
                Collections.unmodifiableMap(new EnumMap<>(Objects.requireNonNull(entityDefaults)));
    }

    protected AbstractSettingDataType(@Nonnull T defaultValue) {
        this(defaultValue, Collections.emptyMap());
    }

    /**
     * Returns default value for this setting.
     *
     * @return default value
     */
    @Nonnull
    protected T getDefault() {
        return defaultValue;
    }

    /**
     * Returns map of EntityType -> type specific defaults, where entity type is represented by its
     * ordinal number (integer).
     *
     * @return mapping from entity type (integer) to type-specific default.
     */
    @Nonnull
    protected Map<Integer, T> getEntityDefaults() {
        return entityDefaults.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().ordinal(), Entry::getValue));
    }

    @Nonnull
    @Override
    public T getDefault(@Nonnull EntityType entityType) {
        final Optional<T> entityDefault =
                Optional.ofNullable(entityDefaults.get(Objects.requireNonNull(entityType)));
        return entityDefault.orElse(defaultValue);
    }

}

