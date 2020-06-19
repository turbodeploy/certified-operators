package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.setting.RangeApiDTO;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Responsible for loading Setting styles from a file at system startup.
 *
 */
public class SettingSpecStyleMappingLoader {

    private static final Logger logger = LogManager.getLogger(SettingsManagerMappingLoader.class);

    private final SettingSpecStyleMapping mapping;

    public SettingSpecStyleMappingLoader(@Nonnull final String settingSpecStyleJsonFileName)
            throws IOException {
        this.mapping = loadSettingSpecStyles(settingSpecStyleJsonFileName);
    }

    @Nonnull
    private SettingSpecStyleMapping loadSettingSpecStyles(@Nonnull final String specStyleJsonFile) {
        logger.info("Loading Setting Specs UI Display Style information from {}", specStyleJsonFile);
        try (InputStream inputStream = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(specStyleJsonFile);
            InputStreamReader reader = new InputStreamReader(inputStream)) {
            final SettingSpecStyleMapping mapping =
                    ComponentGsonFactory.createGson().fromJson(reader, SettingSpecStyleMapping.class);
            logger.info("Successfully loaded Setting Styles from {}", specStyleJsonFile);
            return mapping;
        } catch (IOException e) {
            throw new RuntimeException("Unable to load setting styles.", e);
        }
    }

    @Nonnull
    public SettingSpecStyleMapping getMapping() {
        return mapping;
    }

    /**
     *  JAVA POJO representing the mapping from SettingSpecStyle to the
     *  Setting name.
     *
     *  This mapping has additional information required by the UI to display
     *  the settings properly. Since this information deals with only how the data is
     *  displayed/presented in the UI, we put it in a separate object instead of in the
     *  settingSpecs.
     *
     */
    public static class SettingSpecStyleMapping {

        /**
         * (settingSpec name) -> Setting Style Info.
         */
        private final Map<String, SettingSpecStyleInfo> settingSpecStyleBySpecName;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private SettingSpecStyleMapping() {
            settingSpecStyleBySpecName = new HashMap<>();
        }

        /**
         * Get setting spec style info given its spec name.
         *
         * @param specName Name of the setting spec.
         * @return Return true if the setting spec has style info associated
         *         with it.
         */
        public boolean hasStyleInfo(@Nonnull String specName) {
            return settingSpecStyleBySpecName.containsKey(specName);
        }

        /**
         * Get setting spec style info given its spec name.
         *
         * @param specName Name of the setting spec.
         * @return Return the Optional {@link SettingSpecStyleInfo} for the setting.
         */
        @Nonnull
        public Optional<SettingSpecStyleInfo> getStyleInfo(@Nonnull String specName) {
            if (hasStyleInfo(specName)) {
                return Optional.of(settingSpecStyleBySpecName.get(specName));
            } else {
                return Optional.empty();
            }
        }

    }

    /**
     * Style information for a Setting.
     */
    public static final class SettingSpecStyleInfo {
        private final Range range;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private SettingSpecStyleInfo() {
            range = new Range();
        }

        @Nonnull
        Range getRange() {
            return range;
        }
    }

    /**
     *  Class used to convert the Setting range value to
     *  RangeApiDTO which is used for UI/API presentation as
     *  slider values.
     *
     */
    static final class Range {

        /**
         *  The increment by which the UI slider can move.
         */
        private final double step;

        /**
         * The custom step values by which the UI slider can move.
         */
        private final List<Integer> customStepValues;

        /**
         * The lables corresponding to the max and min values of the range.
         *
         */
        private final List<String> labels;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private Range() {
            this.step = 0;
            this.customStepValues = new ArrayList<>();
            this.labels = new ArrayList<>();
        }

        double getStep() {
            return step;
        }

        List<Integer> getCustomStepValues() {
            return new ArrayList<Integer>(customStepValues);
        }

        List<String> getLabels() {
            return new ArrayList<String>(labels);
        }

        @Nonnull
        RangeApiDTO getRangeApiDTO() {
            RangeApiDTO rangeDTO = new RangeApiDTO();
            rangeDTO.setStep(step);
            rangeDTO.setCustomStepValues(customStepValues);
            rangeDTO.setLabels(labels);
            return rangeDTO;
        }
    }
}
