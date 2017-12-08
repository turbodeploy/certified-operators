package com.vmturbo.group.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;

/**
 * Enumeration for all the pre-built global settings.
 */
public enum GlobalSettingSpecs {

    /**
     * Rate of resize. This is global setting. But in the UX it shows up
     * under VM entity.
     */
    RateOfResize("RATE_OF_RESIZE", "Rate of Resize",
            numeric(1.0f/*min*/, 3.0f/*max*/, 2.0f/*default*/),
            //path is needed for the UI to display this setting in a separate category
            Arrays.asList("resizeRecommendationsConstants"));

    /**
     * Setting name to setting enumeration value map for fast access.
     */
    private static final Map<String, GlobalSettingSpecs> SETTINGS_MAP;

    private final String name;
    private final String displayName;
    private final SettingDataStructure<?> value;
    private final List<String> categoryPath;

    static {
        final GlobalSettingSpecs[] settings = GlobalSettingSpecs.values();
        final Map<String, GlobalSettingSpecs> result = new HashMap<>(settings.length);
        for (GlobalSettingSpecs setting : settings) {
            result.put(setting.getSettingName(), setting);
        }
        SETTINGS_MAP = Collections.unmodifiableMap(result);
    }

    GlobalSettingSpecs(@Nonnull String name, @Nonnull String displayName,
            @Nonnull SettingDataStructure value,
            @Nonnull List<String> categoryPath) {

        this.name = Objects.requireNonNull(name);
        this.displayName = Objects.requireNonNull(displayName);
        this.categoryPath = Objects.requireNonNull(categoryPath);
        this.value = Objects.requireNonNull(value);
    }

    /**
     * Returns setting name, identified by this enumeration value.
     *
     * @return setting name
     */
    @Nonnull
    public String getSettingName() {
        return name;
    }

    /**
     * Finds a setting (enumeration value) by setting name.
     *
     * @param settingName setting name
     * @return setting enumeration value or empty optional, if not setting found by the name
     * @throws NullPointerException if {@code settingName} is null
     */
    @Nonnull
    public static Optional<GlobalSettingSpecs> getSettingByName(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return Optional.ofNullable(SETTINGS_MAP.get(settingName));
    }

    /**
     * Constructs Protobuf representation of setting specification.
     *
     * @return Protobuf representation
     */
    @Nonnull
    public SettingSpec createSettingSpec() {
        final SettingSpec.Builder builder = SettingSpec.newBuilder()
                .setName(name)
                .setDisplayName(displayName)
                .setGlobalSettingSpec(
                    GlobalSettingSpec.newBuilder().build());
        if (hasCategoryPath()) {
            builder.setPath(createCategoryPath());
        }
        value.build(builder);
        return builder.build();
    }

    private boolean hasCategoryPath() {
        return (categoryPath != null && !categoryPath.isEmpty());
    }

    /**
     * Method constructs setting category path object from the {@link #categoryPath} variable.
     *
     * @return {@link SettingCategoryPath} object.
     */
    @Nonnull
    private SettingCategoryPath createCategoryPath() {
        final ListIterator<String> categoryIterator =
                categoryPath.listIterator(categoryPath.size());
        SettingCategoryPathNode childNode = null;
        while (categoryIterator.hasPrevious()) {
            final SettingCategoryPathNode.Builder nodeBuilder =
                    SettingCategoryPathNode.newBuilder().setNodeName(categoryIterator.previous());
            if (childNode != null) {
                nodeBuilder.setChildNode(childNode);
            }
            childNode = nodeBuilder.build();
        }
        final SettingCategoryPath.Builder builder = SettingCategoryPath.newBuilder();
        if (childNode != null) {
            builder.setRootPathNode(childNode);
        }
        return builder.build();
    }

    @Nonnull
    private static SettingDataStructure<?> numeric(float min, float max, float defaultValue) {
        return new NumericSettingDataType(min, max, defaultValue);
    }
}
