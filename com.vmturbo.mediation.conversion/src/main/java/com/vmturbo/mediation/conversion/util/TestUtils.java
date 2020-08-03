package com.vmturbo.mediation.conversion.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.zip.ZipInputStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

public class TestUtils {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Parses a file and constructs a discovery response object.
     *
     * @param filePath path of the file to be parsed.
     * @return discovery response; response with error if file cannot be parsed.
     */
    public static DiscoveryResponse readResponseFromFile(@Nonnull String filePath) {
        // try to parse as binary
        try (final InputStream fis = getInputStream(filePath)) {
            return DiscoveryResponse.parseFrom(fis);
        } catch (InvalidProtocolBufferException e) {
            // failed to parse as binary; fall through to text parsing
            logger.warn("InvalidProtocolBufferException parse as binary: msg=" + e.getMessage(), e);
        } catch (IOException p) {
            logger.warn("IOException parse as binary: msg=" + p.getMessage(), p);
            return DiscoveryResponse.newBuilder().build();
        } catch (Exception e) {
            logger.warn("Exception parse as binary: msg=" + e.getMessage(), e);
        }

        // try to parse as text
        try (final InputStream fis = getInputStream(filePath)) {
            final String drText = IOUtils.toString(fis, Charset.defaultCharset());
            final DiscoveryResponse.Builder builder = DiscoveryResponse.newBuilder();
            TextFormat.getParser().merge(drText, builder);
            return builder.build();
        } catch (IOException p) {
            logger.warn("IOException parse as text: msg=" + p.getMessage(), p);
            return DiscoveryResponse.newBuilder().build();
        }
    }

    // get an input stream, creating a zipinputstream for .zip files
    private static InputStream getInputStream(@Nonnull String filePath) throws IOException {
        File file = new File(filePath);
        FileInputStream fis = new FileInputStream(file);
        // if this is a zip file, get the input stream as a zip.
        if (filePath.endsWith("zip")) {
            // We assume a single entry in the zip file.
            ZipInputStream zis = new ZipInputStream(fis);
            zis.getNextEntry();
            return zis;
        }
        return fis;
    }
}
