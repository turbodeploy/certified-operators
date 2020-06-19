package com.vmturbo.components.api.test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nonnull;

/**
 * Utility class to get paths to resources in a platform-independent way.
 */
public class ResourcePath {

    private ResourcePath() {

    }

    /**
     * DO NOT USE THIS METHOD OUTSIDE OF TESTS.
     * In a .jar file the resources won't load.
     *
     * @param clazz The caller class.
     * @param path The path within the "resources".
     * @return The {@link Path} for the file.
     */
    @Nonnull
    public static Path getTestResource(@Nonnull final Class<?> clazz, @Nonnull final String path) {
        try {
            final URL url = clazz.getClassLoader().getResource(path);
            if (url == null) {
                throw new IllegalArgumentException("Unable to find resource file: " + path);
            }
            // Using Paths.get() on the URI for cross-platform portability.
            // See: https://stackoverflow.com/questions/6164448/convert-url-to-normal-windows-filename-java
            return Paths.get(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Expected valid URI.", e);
        }
    }
}
