package com.vmturbo.deserializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;

import net.jpountz.lz4.LZ4FrameInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Class to deserialize discovery dumps from a binary format to a human readable format.
 */
public class Deserializer {
    private Deserializer() {
    }

    /**
     * Input directory with binary files.
     * @param args the paths to the directory containing the binary files and the output directory
     */
    public static void main(String[] args)  {
        if (args.length != 2) {
            System.out.println("Please provide the path to the directory with the binary files "
                + "and the path to the output directory");
            return;
        }
        deserialize(args[0], args[1]);
    }

    /**
     * Class to deserialize discovery dumps from a binary format to a human readable format.
     * @param sourcePath path to the directory with the binary files
     * @param targetPath output directory with the readable discoveries
     */
    public static void deserialize(String sourcePath, String targetPath) {
        final File binaryDirectory = new File(sourcePath);
        final File textDirectory = new File(targetPath);
        if (!binaryDirectory.exists()) {
            System.out.println("Directory with binary files does not exist. Please provide the "
                + "filepath with the directory with the binary files");
            return;
        }
        if (!textDirectory.exists()) {
            textDirectory.mkdir();
        }
        final String[] allDiscoveryDumpFiles = binaryDirectory.list();
        if (allDiscoveryDumpFiles == null) {
            System.out.println("Cannot get the list of discovery dump files");
            return;
        }
        for (String filename : allDiscoveryDumpFiles) {

            File binaryFile = new File(Objects.requireNonNull(binaryDirectory), filename);
            final DiscoveryResponse response;
            try {
                response = DiscoveryResponse.parseFrom(readCompressedFile(binaryFile));
                File readableFile = new File(Objects.requireNonNull(textDirectory),
                    FilenameUtils.getBaseName(filename) + ".txt");
                System.out.println("Converting " + filename + "to a readable format");
                FileUtils.writeByteArrayToFile(readableFile,
                    response.toString().getBytes());
            } catch (IOException e) {
                System.out.println("Could not read discovery response from file {}");
            }
        }
    }

    private static byte[] readCompressedFile(File file) throws IOException {
        LZ4FrameInputStream gis = new LZ4FrameInputStream(new FileInputStream(file));
        ByteArrayOutputStream fos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = gis.read(buffer)) != -1) {
            fos.write(buffer, 0, len);
        }
        fos.close();
        gis.close();
        return fos.toByteArray();
    }

}
