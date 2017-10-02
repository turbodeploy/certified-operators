package com.turbonomic.telemetry;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.FileUtils;

/**
 * The {@link TelemetryUnpacker} implements decrypting and unpacking the archives.
 */
class TelemetryUnpacker {
    /**
     * The symmetric key algorithm.
     */
    private static final String ALGORITHM = "AES";

    /**
     * The cipher.
     */
    private static final String CIPHER = "AES/GCM/NoPadding";

    /**
     * The GCMParameterSpec tag length.
     */
    private static final int GCM_TAG_LENGTH = 128;

    /**
     * The raw key and nonce length in bytes.
     */
    private static final int KEY_NONCE_LENGTH_BYTES = 16;

    /**
     * The RSA decrypt function.
     */
    private final Function<byte[], byte[]> decryptRSA_;

    /**
     * Constructs the unpacker.
     *
     * @param decryptRSA The decrypt function.
     */
    public TelemetryUnpacker(final Function<byte[], byte[]> decryptRSA) {
        decryptRSA_ = decryptRSA;
    }

    /**
     * Copies the file data out of the zip archive.
     *
     * @param in  The zip input stream.
     * @param out The output stream.
     * @throws IOException In case of an error extracting or writing the data.
     */
    private void copyFileData(final @Nonnull InputStream in, final @Nonnull OutputStream out)
            throws IOException {
        try {
            DataInputStream dis = new DataInputStream(in);
            int length = dis.readInt();
            byte[] keyAndNonce = new byte[length];
            dis.readFully(keyAndNonce);
            byte[] raw = decryptRSA_.apply(keyAndNonce);
            byte[] rawKey = new byte[KEY_NONCE_LENGTH_BYTES];
            byte[] nonce = new byte[KEY_NONCE_LENGTH_BYTES];
            System.arraycopy(raw, 0, rawKey, 0, rawKey.length);
            System.arraycopy(raw, rawKey.length, nonce, 0, nonce.length);

            SecretKey keySpec = new SecretKeySpec(rawKey, ALGORITHM);
            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH, nonce));
            byte[] buffer = new byte[8192];
            for (int cbRead = in.read(buffer); cbRead >= 0; cbRead = in.read(buffer)) {
                out.write(cipher.update(buffer, 0, cbRead));
            }
            out.write(cipher.doFinal());
        } catch (GeneralSecurityException | SecurityException e) {
            throw new IOException(e);
        }
    }

    /**
     * Unpacks the file.
     *
     * @param fileName The file name.
     * @throws IOException In case of an error unpacking the file.
     */
    public File unpack(final @Nonnull String fileName) throws IOException {
        File file = new File(fileName);
        if (!file.isFile() || !file.canRead()) {
            throw new IOException("File does not exist and can't be read.");
        }
        String baseName = file.getName();
        File zipDir = new File(file.getParentFile().getAbsolutePath() + "/" +
                               baseName.substring(0, baseName.lastIndexOf('.')));
        FileUtils.deleteDirectory(zipDir);
        FileUtils.forceMkdir(zipDir);
        try (ZipInputStream in = new ZipInputStream(new FileInputStream(file))) {
            for (ZipEntry ze = in.getNextEntry(); ze != null; ze = in.getNextEntry()) {
                String name = ze.getName();
                // Skip the data we don't need.
                if (!name.endsWith("_topology.zip")) {
                    in.closeEntry();
                    continue;
                }
                try (FileOutputStream out = new FileOutputStream(zipDir.getAbsolutePath() +
                                                                 "/" + name)) {
                    copyFileData(in, out);
                } finally {
                    in.closeEntry();
                }
            }
        }
        return zipDir;
    }
}
