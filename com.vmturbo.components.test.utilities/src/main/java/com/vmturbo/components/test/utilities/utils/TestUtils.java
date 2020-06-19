package com.vmturbo.components.test.utilities.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

/**
 * General-purpose utils for use by the component tests..
 */
public class TestUtils {

    private TestUtils() {}

    /**
     * Retrieve a stream of protobuf messages of the provided type from a ZIP file.
     *
     * @param fileName The target ZIP file. The file should have exactly one entry, which should
     *                 contain the JSON-encoded protobuf messages, one per line.
     * @param builderSupplier The function to use to create a builder (the .newBuilder() static
     *                        function on the protobuf message type).
     * @param <Msg> The specific protobuf message in the ZIP file.
     * @return A stream of {@link Msg} created from the JSON lines in the de-zipped fileName.
     *
     * @throws IOException If there is a problem reading from the file.
     */
    public static <Msg extends AbstractMessage> Stream<Msg> messagesFromJsonZip(
            @Nonnull final String fileName, @Nonnull final Supplier<Builder> builderSupplier)
            throws IOException {

        if (!fileName.endsWith(".zip")) {
            throw new IllegalArgumentException("Expecting zip file to end with .zip! Got: "
                    + fileName);
        }

        final URI filePath;
        try {
            filePath = TestUtils.class.getClassLoader().getResource(fileName).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Got invalid URI from resource.", e);
        }

        final ZipFile zipFile = new ZipFile(Paths.get(filePath).toAbsolutePath().toString());
        final ZipEntry entry = zipFile.stream().findFirst().get();
        final InputStream inputStream = zipFile.getInputStream(entry);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        return reader.lines()
                .map(line -> {
                    AbstractMessage.Builder builder = builderSupplier.get();
                    try {
                        JsonFormat.parser().merge(line, builder);
                        return (Msg)builder.build();
                    } catch (InvalidProtocolBufferException e) {
                        throw new IllegalArgumentException("Exception parsing JSON file.", e);
                    }
                });
    }
}
