package com.vmturbo.group.setting;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;

/**
 *  Loads the default GlobalSettingSpecs and persists the Global Settings in the database.
 */
public class DefaultGlobalSettingsCreator implements Runnable {

    private final Logger logger = LogManager.getLogger(DefaultGlobalSettingsCreator.class);
    private final SettingStore settingStore;
    private final SettingSpecStore specStore;
    private final long timeBetweenIterationsMs;

    public DefaultGlobalSettingsCreator(@Nonnull final SettingSpecStore specStore,
            @Nonnull final SettingStore settingStore,
            final long timeBetweenIterationsMs) {
        Objects.requireNonNull(specStore);
        this.specStore = specStore;
        this.settingStore = Objects.requireNonNull(settingStore);
        this.timeBetweenIterationsMs = timeBetweenIterationsMs;
    }

    /**
     * Creates the default global settings in the {@link SettingStore}.
     * Exits after all default global settings are created, or fail to be created
     * with unrecoverable errors.
     */
    @Override
    public void run() {
        logger.info("Loading Default Global Settings");
        try {
            while (true) {
                try {
                    settingStore.insertGlobalSettings(
                        defaultSettingsFromSpecs(specStore.getAllGlobalSettingSpecs()));
                    logger.info("Done creating default Global Settings.");
                    break;
                } catch (DataAccessException dae) {
                    // TODO: karthikt - For now we retry on every type of DataAccessException.
                    // Would be better to check for subtypes of DB exceptions which are
                    // recoverable or transient errors and retry only on these errors.
                    logger.error("Failed to create default GlobalSettingSpecs. Retrying", dae);
                } catch (InvalidProtocolBufferException ipe) {
                    logger.error("Failed to create default GlobalSettingSpecs", ipe);
                    // Can't automatically recover from this errro. So no point
                    // re-trying.
                    break;
                }
                Thread.sleep(timeBetweenIterationsMs);
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted creation of global settings", e);
            Thread.currentThread().interrupt();
        }
    }

    @Nonnull
    static List<Setting> defaultSettingsFromSpecs(
            @Nonnull final Collection<SettingSpec> specs) {

        List<Setting> settings = new LinkedList<>();

        for (SettingSpec spec : specs) {
            final Setting.Builder retBuilder = Setting.newBuilder().setSettingSpecName(spec.getName());
            switch (spec.getSettingValueTypeCase()) {
                case BOOLEAN_SETTING_VALUE_TYPE: {
                    final BooleanSettingValueType valueType = spec.getBooleanSettingValueType();
                    retBuilder.setBooleanSettingValue(BooleanSettingValue.newBuilder()
                            .setValue(valueType.getDefault()));
                    break;
                }
                case NUMERIC_SETTING_VALUE_TYPE: {
                    final NumericSettingValueType valueType = spec.getNumericSettingValueType();
                    retBuilder.setNumericSettingValue(NumericSettingValue.newBuilder()
                            .setValue(valueType.getDefault()));
                    break;
                }
                case STRING_SETTING_VALUE_TYPE: {
                    final StringSettingValueType valueType = spec.getStringSettingValueType();
                    retBuilder.setStringSettingValue(StringSettingValue.newBuilder()
                            .setValue(valueType.getDefault()));
                    break;
                }
                case ENUM_SETTING_VALUE_TYPE: {
                    final EnumSettingValueType valueType = spec.getEnumSettingValueType();
                    retBuilder.setEnumSettingValue(EnumSettingValue.newBuilder()
                            .setValue(valueType.getDefault()));
                    break;
                }
                default: {
                    throw new RuntimeException("Setting spec " + spec.getName() +
                            " is not properly formatted - no value type set!");
                }
            }

            settings.add(retBuilder.build());
        }
        return settings;
    }
}
