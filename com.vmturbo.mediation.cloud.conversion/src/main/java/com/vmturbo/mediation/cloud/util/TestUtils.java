package com.vmturbo.mediation.cloud.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    /**
     * Get the old provider types for the entity before conversion.
     */
    public static List<EntityType> getOldProviderTypes(@Nonnull EntityDTO entityDTO,
                                                 @Nonnull CloudDiscoveryConverter converter) {
        return entityDTO.getCommoditiesBoughtList().stream()
                .map(commodityBought -> converter.getRawEntityDTO(
                        commodityBought.getProviderId()).getEntityType()
                ).collect(Collectors.toList());
    }

    /**
     * Get the new provider types for the entity after conversion.
     */
    public static List<EntityType> getNewProviderTypes(@Nonnull EntityDTO.Builder entityBuilder,
                                                 @Nonnull CloudDiscoveryConverter converter) {
        return entityBuilder.getCommoditiesBoughtList().stream()
                .map(commodityBought -> converter.getNewEntityBuilder(
                        commodityBought.getProviderId()).getEntityType()
                ).collect(Collectors.toList());
    }

    /**
     * Dump the discovery response into a file in plain text. This is added for better debugging
     * purposes. Sample use case:
     *     dumpDiscoveryResponse(newResponse, "aws_new_discovery_response_eng.txt");
     *
     * @param discoveryResponse the discovery response to dump
     * @param filePath the file path to save the dumped DTOs
     */
    public static void dumpDiscoveryResponse(DiscoveryResponse discoveryResponse, String filePath) {
        final File file = new File(filePath);
        try (final OutputStream os = new FileOutputStream(file)) {
            os.write(discoveryResponse.toString().getBytes(Charset.defaultCharset()));
        } catch (IOException e) {
        }
    }
}
