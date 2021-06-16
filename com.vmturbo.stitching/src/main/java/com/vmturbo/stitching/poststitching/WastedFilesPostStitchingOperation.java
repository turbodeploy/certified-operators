package com.vmturbo.stitching.poststitching;

import java.io.File;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.utilities.WastedFiles;

/**
 * Get all VC Storages and identify the wasted files virtual volume for each by the fact that
 * it is not associated with any virtual machines via connectedFrom.  Files in this volume represent
 * actual wasted files.  Remove files that match the ignoreFiles or ignoreDirectories
 * settings from the list of wasted files.  All files that remain represent wasted files.
 */
public class WastedFilesPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeEntityTypeScope(SDKProbeType.VCENTER.getProbeType(),
                EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
        @Nonnull final Stream<TopologyEntity> entities,
        @Nonnull final EntitySettingsCollection settingsCollection,
        @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        final RegexCompilerAndErrorHandler regexCompiler = new RegexCompilerAndErrorHandler();
        // iterate over storages and update the related wasted files volume
        entities.forEach(storage -> {
            // Get the regex for ignored directories and files from the settings for the storage
            // and, if they exist, create Pattern objects to use for filtering the list of wasted
            // files.  We'll remove any files that match the ignore Patterns.
            final Optional<Setting> ignoreDirectorySetting =
                settingsCollection.getEntitySetting(storage.getOid(),
                    EntitySettingSpecs.IgnoreDirectories);
            final Optional<Setting> ignoreFilesSetting =
                settingsCollection.getEntitySetting(storage.getOid(),
                    EntitySettingSpecs.IgnoreFiles);

            final Optional<Pattern> ignoreDirsPattern = ignoreDirectorySetting.isPresent()
                ? regexCompiler.safePatternCompile(ignoreDirectorySetting.get().getStringSettingValue().getValue()) : Optional.empty();
            final Optional<Pattern> ignoreFilesPattern = ignoreFilesSetting.isPresent()
                ? regexCompiler.safePatternCompile(ignoreFilesSetting.get().getStringSettingValue().getValue()) : Optional.empty();

            if (WastedFiles.getWastedFilesVirtualVolume(storage).isPresent()) {
                final TopologyEntity wastedFilesVolume =
                    WastedFiles.getWastedFilesVirtualVolume(storage).get();
                final Set<VirtualVolumeFileDescriptor> keepFiles =
                    getFilesToKeepForVirtualVolume(wastedFilesVolume, ignoreFilesPattern, ignoreDirsPattern);
                resultBuilder.queueUpdateEntityAlone(wastedFilesVolume,
                    entityToUpdate -> updateFilesForVirtualVolume(entityToUpdate, keepFiles));
            }
        });

        if (regexCompiler.hasCompilationErrors()) {
            regexCompiler.logCompilationErrors();
        }
        return resultBuilder.build();
    }

    /**
     * Overwrite the files of a Virtual Volume with the given set of files.
     *
     * @param virtualVolume to update
     * @param files to overwrite to the volume
     */
    private void updateFilesForVirtualVolume(@Nonnull final TopologyEntity virtualVolume,
                                             @Nonnull final Set<VirtualVolumeFileDescriptor> files) {
        virtualVolume.getTopologyEntityDtoBuilder().getTypeSpecificInfoBuilder()
            .getVirtualVolumeBuilder().clearFiles().addAllFiles(files);
    }

    /**
     * Get a set of files for Virtual Volume that should NOT be ignored.
     *
     * @param virtualVolume the virtual volume
     * @param ignoreFilesPattern the {@link Pattern} to ignore the files
     * @param ignoreDirsPattern the {@link Pattern} to ignore the directory
     * @return the files that should NOT be ignored
     */
    private Set<VirtualVolumeFileDescriptor> getFilesToKeepForVirtualVolume(@Nonnull final TopologyEntity virtualVolume,
                                                                            @Nonnull final Optional<Pattern> ignoreFilesPattern,
                                                                            @Nonnull final Optional<Pattern> ignoreDirsPattern) {
        return virtualVolume.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesList().stream()
            .filter(file -> !shouldFileBeIgnored(file, ignoreFilesPattern, ignoreDirsPattern))
            .collect(Collectors.toSet());
    }

    /**
     * A file should NOT be ignored if the path for the file is not being ignored according to
     * the filesPattern AND all the {@link VirtualVolumeFileDescriptor#getLinkedPathsList()} are
     * not matched by the files patterns.
     *
     * @param file the {@link VirtualVolumeFileDescriptor}
     * @param ignoreFilesPattern pattern for ignoring the files
     * @param ignoreDirsPattern pattern for ignoring the directoris
     * @return whether or not the file should be ignored
     */
    private boolean shouldFileBeIgnored(@Nonnull final VirtualVolumeFileDescriptor file,
                                        @Nonnull final Optional<Pattern> ignoreFilesPattern,
                                        @Nonnull final Optional<Pattern> ignoreDirsPattern ) {
        if (isIgnored(file.getPath(), ignoreFilesPattern, ignoreDirsPattern)) {
            return true;
        }
        for (String linkedPaths : file.getLinkedPathsList()) {
            if (isIgnored(linkedPaths, ignoreFilesPattern, ignoreDirsPattern)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the path is ignored according to global wildcard settings.
     * Storage browsing probes are required to return Unix paths.
     *
     * @param path Unix-style separated full pathname
     * @param ignoreDirs ignore directories pattern
     * @param ignoreFiles ignore files pattern
     * @return true if the path should be ignored
     */
    private static boolean isIgnored(@Nonnull final String path,
                                     @Nonnull final Optional<Pattern> ignoreFiles,
                                     @Nonnull final  Optional<Pattern> ignoreDirs) {
        File f = new File(path);
        if (ignoreFiles.isPresent() && ignoreFiles.get().matcher(f.getName()).matches()) {
            return true;
        }
        if (!ignoreDirs.isPresent() || ignoreDirs.get().pattern().isEmpty()) {
            return false;
        }
        File parent = f.getParentFile();
        while (parent != null) {
            if (ignoreDirs.get().matcher(parent.getName()).matches()) {
                return true;
            }
            parent = parent.getParentFile();
        }
        return false;
    }

    /**
     * Class that compiles and validates regular expressions. It also has the additional function
     * to count how many compilation errors it has encountered and log them
     */
    @VisibleForTesting
    static class RegexCompilerAndErrorHandler {

        private final Set<String> compilationErrors = new HashSet<>();

        /**
         * Check that the regex the user specified is a valid regex pattern.  Suppress a
         * {@link PatternSyntaxException} if it occurs.
         *
         * @param patternRegex to create a {@link Pattern} from.
         * @return {@link Optional<Pattern>} if the regex compiles, Optional.empty otherwise.
         */
        public Optional<Pattern> safePatternCompile(@Nonnull String patternRegex) {
            try {
                return Optional.of(Pattern.compile(patternRegex));
            } catch (PatternSyntaxException e) {
                logger.debug("Could not compile pattern for regex {} when processing wasted files.",
                    patternRegex, e);
                compilationErrors.add(patternRegex);
                return Optional.empty();
            }
        }

        /**
         * Returns whether or not there are compilation errors.
         * @return true if there are errors
         */
        public boolean hasCompilationErrors() {
            return compilationErrors.size() > 0;
        }

        /**
         * Logs the number of compilation errors.
         * @return the error message
         */
        public String logCompilationErrors() {
            final String errorMessage = "Could not compile the following regex patterns when processing wasted files: "
                    + String.join(", ", compilationErrors);
            logger.error(errorMessage);
            return errorMessage;
        }
    }
}
