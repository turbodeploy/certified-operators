package com.vmturbo.components.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Interesting properties about the build used to create this version of the code,
 * loaded from the git properties file generated at build-time by the git-commit-id plugin.
 */
@Immutable
public class BuildProperties {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The path to the git commit file, relative to the classpath.
     * Must be in sync with the configuration of the git-commit-id-plugin in build/pom.xml.
     */
    private static final String GIT_PROPERTIES_PATH = "git.properties";

    /**
     * The prefix used for build properties.
     * Must be in sync with the configuration of the git-commit-id-plugin in build/pom.xml
     */
    private static final String PREFIX = "turbo-version";

    private static final String UNKNOWN = "Unknown";

    // START - the names of the properties we care about.
    private static final String BRANCH = PREFIX + ".branch";
    private static final String DIRTY = PREFIX + ".dirty";
    private static final String VERSION = PREFIX + ".build.version";
    private static final String BUILD_TIME = PREFIX + ".build.time";
    private static final String COMMIT_ID = PREFIX + ".commit.id";
    private static final String SHORT_COMMIT_ID = COMMIT_ID + ".abbrev";
    // END - the names of the properties we care about.

    /**
     * The properties loaded from the git.properties file.
     */
    private final String branch;
    private final String version;
    private final String buildTime;
    private final String commitId;
    private final String shortCommitId;
    private final boolean dirty;

    private static final BuildProperties INSTANCE = new BuildProperties();

    /**
     * Retrieve the {@link BuildProperties} used for this component.
     *
     * @return The {@link BuildProperties} instance.
     */
    public static BuildProperties get() {
        return INSTANCE;
    }

    private BuildProperties() {
        final Properties properties = new Properties();
        try (InputStream configPropertiesStream = BuildProperties.class.getClassLoader()
            .getResourceAsStream(GIT_PROPERTIES_PATH)) {
            if (configPropertiesStream != null) {
                properties.load(configPropertiesStream);
            } else {
                logger.warn("Cannot find git properties file: {} in class path", GIT_PROPERTIES_PATH);
            }
        } catch (IOException e) {
            // if the component defaults cannot be found we still need to send an empty
            // default properties to ClusterMgr where the global defaults will be used.
            logger.warn("Cannot read git properties file: {}", GIT_PROPERTIES_PATH);
        }

        branch = Optional.ofNullable(properties.getProperty(BRANCH)).orElse(UNKNOWN);
        version = Optional.ofNullable(properties.getProperty(VERSION)).orElse(UNKNOWN);
        buildTime = Optional.ofNullable(properties.getProperty(BUILD_TIME)).orElse(UNKNOWN);
        commitId = Optional.ofNullable(properties.getProperty(COMMIT_ID)).orElse(UNKNOWN);
        shortCommitId = Optional.ofNullable(properties.getProperty(SHORT_COMMIT_ID)).orElse(UNKNOWN);
        dirty = Boolean.valueOf(properties.getProperty(DIRTY));
    }

    @Nonnull
    public String getBranch() {
        return branch;
    }

    @Nonnull
    public String getVersion() {
        return version;
    }

    @Nonnull
    public String getBuildTime() {
        return buildTime;
    }

    @Nonnull
    public String getCommitId() {
        return commitId;
    }

    @Nonnull
    public String getShortCommitId() {
        return shortCommitId;
    }

    public boolean isDirty() {
        return dirty;
    }

    @Override
    public String toString() {
        return ComponentGsonFactory.createGson().toJson(this);
    }
}
