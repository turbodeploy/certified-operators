package com.vmturbo.components.common.setting;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

    /**
     * Constructor of {@link AbstractSettingDataType}.
     *
     * @param defaultValue the default value for this setting or null if there is no specified
     * default value.
     * @param entityDefaults map of default values for different entity types
     */
    protected AbstractSettingDataType(@Nullable T defaultValue,
                                      @Nonnull Map<EntityType, T> entityDefaults) {
        this.defaultValue = defaultValue;
        this.entityDefaults = entityDefaults.isEmpty() ? Collections.emptyMap() :
                Collections.unmodifiableMap(new EnumMap<>(Objects.requireNonNull(entityDefaults)));
    }

    protected AbstractSettingDataType(@Nullable T defaultValue) {
        this(defaultValue, Collections.emptyMap());
    }

    /**
     * Returns default value for this setting or null if there is no specified default value.
     *
     * @return default value
     */
    @Nullable
    protected T getDefault() {
        return defaultValue;
    }

    @Nullable
    @Override
    public T getDefault(@Nonnull EntityType entityType) {
        final Optional<T> entityDefault =
                Optional.ofNullable(entityDefaults.get(Objects.requireNonNull(entityType)));
        return entityDefault.orElse(defaultValue);
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
}

