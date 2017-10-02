package com.turbonomic.telemetry;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.annotation.Nonnull;
import javax.crypto.Cipher;

import com.amazonaws.regions.Regions;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;

/**
 * The {@link Driver} implements the main loop.
 */
public class Driver {
    /**
     * The serialization character set.
     */
    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * Good private key.
     */
    private static final String PRIVATEKEY =
            "MIIG/QIBADANBgkqhkiG9w0BAQEFAASCBucwggbjAgEAAoIBgQDCyeGnFAJyjRoHBr3PVdzu" +
            "+kZrsGCXk8yQPiNMP1pUkEzhjxVNFceLbK3eoHslFYV2XDQvAvY" +
            "+aTml1BQxIOmEhOWKjmeRjs1sBcyTzXvqcjxP1j+H3PsB3pdnFXkNuAQO4I+bBQ96nweL8kDAJpaccVt" +
            "/5CXXF6FQEe+VSreErYT0v8gFKoBZ6E0Fgx+z5X0BnB4oKh8NgMwByyGP" +
            "+YVMz1k2a8mdeqV755qxofbmT2bIuw1E+WKPY/VfYe8WuoaU+lmL1ZusrcA/+q2rOat5Xexwq" +
            "+WvNBFEPzPh6ngzULII7NVjqC4Yo8fPMIeQ5XvD/hRFZW5kc9M" +
            "+mJxBvJjvSYa1HwZNWfVRzJohYJzbsjbJh49FD" +
            "/9SZFqRNvIHKGTDAHc1tFSzEVaB4cBzxjEj2ax2GWIFrZHdvSuHj5+qsUj0Mlzjy7TkvZt0Pbaa" +
            "+vVK0CX7mXoYNBStfT2zetTwmBmc5NjSu6P4YAGdd9bsZPgvriWVfrjFvKUkMZKMfWkCAwEAAQKCAYB" +
            "+WdPMu/cGdS6v5hiTeD/SqHerfQJ68D1eR2BAIkwm1F96ZITNttX0Q6" +
            "/Y3EJlALvM1exnnkU6oKjrP1FFuPKgqk/2" +
            "+36E9ZuceXqFWWIK5sTmiLQ3HgmX4dpV3KDkXDwNpm6ggLzUrnmHtsF3otBJdGKE5+n" +
            "/xu01chz1uzyljKTU2d4WM8aEC2KCs9aJq8/Vjn4lUYVJDgXHGAuSR6nG9C0LVwm15AuZ" +
            "/tbiaIwuQs0KYgMufZIILy/ICk" +
            "+7b4uggnOPwK0dkfFa0mq1JbiThpcP5BlVghuP43AF0ZMIYe8ihl54QRm7Woe1" +
            "+/PrgQr5DGKYJ98u8MYtT+gOqVfMgKILPN1ZEyFIt3pstvKknrSbrqtZCSPK4tWWH" +
            "rzMzldn1vuz1m1ktmxa5MDSY2ETRj60Aw1BGd51lrHM7vFKDWT1Mktn18fmO4" +
            "/ONtSXgN9HZcSSzGMazcy+YKhQSB/K7" +
            "/ZulWLUtRtAYT3nmDOxR5j3Bpx3wD5gDZOSNl2HRu0CgcEA77y6e9omIPK1idZQ2WWRxYxP94t" +
            "8QwcnLdANNbiBpSl3NVOAR2gwuw3NGuvMlmVBlI8y8UJBmWhXEX7btqA7yeHGiI1jfLlXd0MKt" +
            "/7fq2VwbwPpQL5bqfcW5VjfpLYEwTMpY9KAH3j0mM8qMDr3isSe" +
            "/DmtKlaD3MgKeSyZKxyA09BXGC6X1d/lW+HPAB3YSlIOKsSO25u+aEUujjFrSpn0G7Y/2KE" +
            "/e3uyctFj5/QTzsCump2oS1MENY5vLamPAoHBANAAk4FKTLKDA42DX8FFg4sH7o/Pdo8QZ" +
            "+3Ej6uQtrRf+MG6jkLY7Hu2UquGz19HXvkToBn357K8Ay0bKkF0IyfMpsrSILXsUQ" +
            "/6TXzrvIE4lnoAYBj4qOD5saB1vFJAuoc47" +
            "/sGAkzvPjLAMjImtBZCTtGlQvr8GfwVt7pn7QmZwwjMocmktvxPQV3a0BljeIWvrA4hLPo3KDs54r" +
            "vmTwTVkXggRlre2oJw7TPBOKMwwCijAx13DxE3EH2OnF89hwKBwQCAIzr1ufsJP1Ei9GdQW4aXtVCEuo" +
            "xXczpQtVRqM00wT0KFQTVedWUehdjsG77FYCe2VPfglg5kBa4MDuc27NwoIsIv" +
            "+IqFfrcuaqoPAG7iuJxo6glBbr2l0gjt2xvmeNd/wG3OChQrPkZWatxc3Re0lzV3EQjqiUV" +
            "/u3hFnj3cGicNj90NctH7IeoAu2gd4tXZm5Qw7dCBtvQF3XxtyiJTxIgCS95CVqWMxefykNK6hu4TY0" +
            "+wGrZ6VEv6qD/qgycCgcBDnjxVhoriaVbpVTD2yptVwjFajO4U2hd" +
            "/Iyw1cOkEQELE79m5f315Ri8f1cZqSfBk9m1Qo9etshMyQad9+bxH" +
            "/u6l9qNtoLJFiHBSUOxJ8dRxOtgCpit4X5PP/xZGgwZgWTEioLR0wD2vD+4BgbV/mvS2kIvyvavQ2FqO" +
            "/aQAP/04ShW6UJKv+S/dy/lkl4Khqxc6zKwrd2zbvBiqTteXUbplm9BjUBkm2IE0FvdLjdiaHUww8vhx9z2" +
            "kI41QJNcCgcAsuvyBg/0gDUXSmLyXOkncYsy721BXrPjMaEnImDwAkyW" +
            "+WlBnzCODf34wa3zzlcGkiA6sz7m4FnE3ALJHbOUqpvED6FdLigfN3RT3" +
            "+FAhYXLgcIIcGJZCVb4Nb9hsw" +
            "+D2dHpgamp2N5UWDoXsX23nDFfRBJWanKBsMSo02186wjw0PuvDCXpAVukor8GTFj5jLuYEgwxJVPxIBethHgr" +
            "l/nDdqyflHqzeU51BYc34ewwKRbpqYtr/ntW0iBfwXEs=";

    /**
     * The AWS S3 Access key.
     */
    private static final String ACCESS_KEY = "AKIAIQ7RIJNU6CJ4W6ZQ";

    /**
     * The AWS S3 Secret key.
     */
    private static final String SECRET_KEY = "+6DdpUVjKA06KBO2G69ASHiYJX/qQsWmjUpmF9w0";

    /**
     * Decrypts the RSA-encrypted byte array with the given private key.
     *
     * @param cipherText The cipher text.
     * @param privateKey The private key to be used.
     * @return The plaintext.
     * @throws SecurityException In case of an error decrypting the ciphertext.
     */
    private byte[] decryptRSA(final @Nonnull byte[] cipherText, final @Nonnull String privateKey)
            throws SecurityException {
        try {
            byte[] keyBytes = privateKey.getBytes("UTF-8");
            PKCS8EncodedKeySpec spec =
                    new PKCS8EncodedKeySpec(Base64.getDecoder().decode(keyBytes));
            KeyFactory kf = KeyFactory.getInstance("RSA");
            PrivateKey key = kf.generatePrivate(spec);
            Cipher cipherRSA = Cipher.getInstance("RSA");
            cipherRSA.init(Cipher.DECRYPT_MODE, key);
            return cipherRSA.doFinal(cipherText);
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    /**
     * Decrypts the RSA-encrypted byte array.
     *
     * @param cipherText The cipher text.
     * @return The plaintext.
     * @throws SecurityException In case of an error decrypting the ciphertext.
     */
    private byte[] decryptRSA(final @Nonnull byte[] cipherText) throws SecurityException {
        return decryptRSA(cipherText, PRIVATEKEY);
    }

    /**
     * Uploads the contents of a single decrypted telemetry file.
     *
     * @param unpackedFiles The list of decrypted and unpacked files.
     * @param index         The elastic search index.
     * @throws IOException In case of an error uploading the data.
     */
    private void upload(final @Nonnull File[] unpackedFiles, final @Nonnull String index)
            throws IOException {
        for (File topology : unpackedFiles) {
            try (ZipInputStream in = new ZipInputStream(new FileInputStream(topology))) {
                for (ZipEntry ze = in.getNextEntry(); ze != null; ze = in.getNextEntry()) {
                    try {
                        if (ze.getName().startsWith("Entities")) {
                            TelemetryParser parser = new TelemetryParser();
                            TelemetryElasticLoader loader = new TelemetryElasticLoader(index);
                            parser.parseEntities(in, loader.getEntitiesProcessor());
                        }
                    } finally {
                        in.closeEntry();
                    }
                }
            }
        }
    }

    /**
     * Performs the cycle.
     * <ol>
     * <li>Download files</li>
     * <li>Unpack files</li>
     * <li>Load data into the elastic search</li>
     * </ol>
     *
     * @throws IOException In case of I/O operation error (any and all of them).
     */
    private void performCycle() throws IOException {
        File tempDir = File.createTempFile("telemetry", "temp");
        FileUtils.forceDelete(tempDir);
        FileUtils.forceMkdir(tempDir);
        try {
            String tempDirPath = tempDir.getCanonicalPath();
            TelemetryRetriever retriever = new TelemetryRetriever(SECRET_KEY, ACCESS_KEY,
                                                                  Regions.US_EAST_1);
            TelemetryUnpacker unpacker = new TelemetryUnpacker(this::decryptRSA);
            // List files in the S3 bucket.
            List<String> files = retriever.listFiles();
            for (String file : files) {
                String targetFileName = tempDirPath + "/" + file;
                // Download file.
                retriever.retrieveFile(file, targetFileName);
                // Decrypt and unpack the base structure of the file.
                final File[] unpackedFiles = unpacker.unpack(targetFileName).listFiles();
                // Figure out the index. It is Base64-encoded, but the elastic search allows for
                // lower case only.
                String index = file.substring(0, file.indexOf('_'));
                byte[] hash = Base64.getDecoder().decode(index.getBytes(CHARSET));
                index = new String(Hex.encodeHex(hash, true));
                // Load the topology data into the elasticsearch.
                upload(Objects.requireNonNull(unpackedFiles), index.toLowerCase());
            }
            // Delete the files.
            retriever.cleanUpBucket(files);
        } finally {
            FileUtils.forceDelete(tempDir);
        }
    }

    /**
     * The main entry point.
     *
     * @param args The program arguments.
     */
    public static void main(String[] args) {
        Driver driver = new Driver();
        long sleepInterval = TimeUnit.HOURS.toMillis(12);
        while (true) {
            // Perform cycle.
            try {
                driver.performCycle();
            } catch (IOException e) {
                System.err.println("Error performing cycle.");
            }
            // Sleep.
            try {
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
