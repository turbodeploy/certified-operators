package com.turbonomic.telemetry;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.annotation.Nonnull;
import javax.crypto.Cipher;

import org.junit.Assert;
import org.junit.Test;

/**
 * The {@link TelemetryUnpackerTestIT} tests the telemetry unpacker.
 */
public class TelemetryUnpackerTestIT {
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
     * The wrong private key.
     */
    private final static String PRIVATEKEY_WRONG =
            "MIIG/AIBADANBgkqhkiG9w0BAQEFAASCBuYwggbiAgEAAoIBgQCQwcLqC2wRyoPeCwP" +
            "/kE93iQlxYCaD9tcv0amYz+rTPb82M+OqrIP8HrjwBuGvJjLe1rihGukLWM" +
            "/6SZEiiZsyW8UjHP4iOEpjxLlvmnoyivX8FKlfom2jwz" +
            "+pb38wOvGmDQZldvO0LSUh5DFGN9tq8RJcY4vcu+djqRBT7LC5b5VMXVrruQr4Ozza0Hyl" +
            "/swvkkJiJEH6cHUpj2Fu482BDHgBSMTtEXJy2QzOon63hg1qTT5fQ18IrUPF1ajgXDZx7" +
            "/+otfJP5EZmKZzP6ENzAX35U5PLTS+RgKwNYrl2IfELyCA6FjjZxQm4LQb+K5IJxIiVLa" +
            "/HpNAYeg2OowKB5vODEx6v/bJQY3H5gIqrEh+cJlqRMPUksQkyu/M4RSlRuLYd0JBgvzw3A" +
            "/TW1BYKCJrrDwC3YRHaesvJ+Z2mCHUiQ3WW5dVfiI7ZbRRxpVH+tXSDWyyb" +
            "+9wrtWBjkZ9VIIEQYjXn85Ar1sL1Rs+Th3fIK+NRd2w46fHMfmAnQ" +
            "+MCAwEAAQKCAYAumvOSPtMwdy81kf5bja7IIYq0wewkmJh3gN1FoctM75hcbyBhluF3jrykKtM" +
            "GfRnrvN38oJlfWdy8DGt53yvkbr9tqqv3gIAS1weM8wNcuniwDEbrz3PVQcuBVbBxasjaWV8x0" +
            "7Zq0A4gd0OWA+3U5ICw1iv1iVnaMFhdbNsJGO/MglH" +
            "/irgJa6kQGgfHud0rg79zsZC8fPB7zPnlpdpILieE4lTkrrVWXKNO8" +
            "+owVCyFiREHwr0aIg6r5zpT4dv1qwKXas" +
            "+NhWrhgRy7oSkrBe9wFI9dvb3pRyRtE4YMhm4XXHaXrSlwSJP4wUjQ02E2iP/St" +
            "/5A8kvcqpE0T1z9zO3EKxU5igTH3UG7MnnUiO6WkI1o8CphTlbbgILBh2w7J19eJ8IledtWWxuIST" +
            "Khxavia+/qNIbm9CdrIWkDtUFHI9f2KNbR6e9iZrmGfunokj12Ye6hsjCjLijA8uyYRv1tlSVq0l3N" +
            "Wu4y+Zk6E+FT+9OwHmU+a56qk6yUYgECgcEA99uKk/lWR5N/WKwGjZ6CZV+GN5EY5LegFY+8KF6zI97" +
            "++0Ogfi+1go0RBjcKgCdIH6zqG1d4Vclvf0wKtQFdWUH" +
            "+po84orgudYvPlvijvTCHn28SiXLbay60FHrU+NcZp9yBmqcQSraY0AwJnoxOtpNuLo" +
            "/fPWP2g97rBflafl+aEShDFozp8t+gUx5G1Aw29jcHA6uxjaEuuyFaN5fDfLL" +
            "/+Eik2osJ1r86Pw85d2mXxEvuxxbTmHTxTfUJt6WjAoHBAJWDJyjcKsHvIPHgasQunZ+fzXZLV" +
            "/qpm6D1Fw5pwYEKFZE9Io2a43ZysM4Lg9Gudia6mvwxWHua3r4VCymacmGlDw4BoAOr73kscmHlv4jCD" +
            "y9YEHHDZgu2KVIwo4mCkYAtUWsNiv2S/WAxZ6kCMtWQq+82pVt1HCttjQKYgG" +
            "+OAZhx7sbN6l81K4YWjo8t2P0vM2ZcF6tj0k55xQ3kDLnC5dZPNI2UriGLF16GQJo8HfEtK" +
            "/af0LfvewIGzM1MwQKBwBGkjfu0fuC/bgwoypqgEtYWc55W9LerHnkKbzFMdGH0SKAoEy+IQ9pUIkcX" +
            "+eZfZXHjxBJIKqeonAgqqIRz4WouGWtPvI9QnvX9CrzLXBRmdPDzXhVsmJPLkkP27lv8K7ZYKt2QUMeQ" +
            "sdX1VWX0xNGYMvqbgR+EkxKV9pLJShi+w1+/Ru8" +
            "+/pQsOORRvmbvq6XhCt3HBe937rShTiOw6NhigXfx69ImnX+swv" +
            "+kHMoUORpe8VM4m705bcfHznZFEwKBwGUbVz0Dfoq9inrnd2wJ1iAfMhOgKfPiBNOZSqk9jVUVG7anMB" +
            "mwt8ffEX+VO+RtZYsVQD/xotObzKugkXbl8hnkREUFhk9VU1GPZB6d" +
            "/qOeECTFGgHE0Np0qobhS4ZvXLPgcaHCe861O1F0t5QMX5IbDpprSvGJRa58nBI4TQqjbqLH0WBx2d" +
            "/elftMf8WFjKrS46CCAKOvkoXtRPv6zzxD96ew0mo4NHWMmRYcFc33f4wcLxHNy5jHg1jAfnkLQQKBwG" +
            "RLLjUdCqphFhB/pebk2NG337KzIEd7LDp8kkDzR4FPvS3fzmFDvIwKvg9pn54WJU2Drgf1fXXI8pUYrOV" +
            "9x4UHkXh4j88wEikV9T6aSXN4THT8oVJ3pkkRm/rS2PRA872ZQOIPaUHqfZC1rSfX" +
            "/B947gVJeq4yErtAb4zTQSsoqwga09EnCWrpYCPV" +
            "+tqdBvqsGd83dcfzBvxonUW3bgHVha1Q93GV8nemRgh78cIhG/tZfzvIQ1liEvJtn8hxlg==";

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
     * Decrypts the RSA-encrypted byte array.
     *
     * @param cipherText The cipher text.
     * @return The plaintext.
     * @throws SecurityException In case of an error decrypting the ciphertext.
     */
    private byte[] decryptRSAWrongKey(final @Nonnull byte[] cipherText) throws SecurityException {
        return decryptRSA(cipherText, PRIVATEKEY_WRONG);
    }

    /**
     * Returns the test file.
     *
     * @return The test file.
     */
    private File getTestFile() {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resourceURL = classLoader.getResource("telemetry_test.zip");
        Assert.assertNotNull(resourceURL);
        String urlAsFile = resourceURL.getFile();
        Assert.assertNotNull(urlAsFile);
        return new File(urlAsFile);
    }

    /**
     * Calculate the bit mask based on a name (name - bit):
     * <ul>
     * <li>Targets   - 1</li>
     * <li>Entities  - 2</li>
     * </ul>
     *
     * @param bitMask The bit mask.
     * @param name    The name to be evaluated.
     * @return The new bit mask.
     */
    private int calculateBitMask(final int bitMask, final @Nonnull String name) {
        if (name.equals("Targets.diags")) {
            return bitMask | 1;
        } else if (name.startsWith("Entities.") && name.endsWith(".diags")) {
            return bitMask | 2;
        }
        return bitMask;
    }

    /**
     * Tests the entities.
     * <pre>
     * {@code
     *
     * $ cat Entities.72091601495072.diags | cut -d'"' -f10 | sort | uniq -c
     *   29 "APPLICATION"
     *    1 "DATACENTER"
     *    7 "DISK_ARRAY"
     *    3 "NETWORK"
     *    8 "PHYSICAL_MACHINE"
     *    7 "STORAGE"
     *    2 "VIRTUAL_DATACENTER"
     *   32 "VIRTUAL_MACHINE"
     * }
     * </pre>
     *
     * @param in the Zip input stream.
     * @throws IOException In case of an error reading the entities.
     */
    private void testEntities(final @Nonnull InputStream in) throws IOException {
        TelemetryParser loader = new TelemetryParser();
        Map<String, Integer> types = new HashMap<>();
        loader.parseEntities(in, (t, id) -> types.compute(t, (k, v) -> v == null ? 1 : v + 1));
        Assert.assertEquals(29L, types.get("APPLICATION").longValue());
        Assert.assertEquals(1L, types.get("DATACENTER").longValue());
        Assert.assertEquals(7L, types.get("DISK_ARRAY").longValue());
        Assert.assertEquals(3L, types.get("NETWORK").longValue());
        Assert.assertEquals(8L, types.get("PHYSICAL_MACHINE").longValue());
        Assert.assertEquals(7L, types.get("STORAGE").longValue());
        Assert.assertEquals(2L, types.get("VIRTUAL_DATACENTER").longValue());
        Assert.assertEquals(32L, types.get("VIRTUAL_MACHINE").longValue());
    }

    /**
     * Tests the unpacking of the archive.
     */
    @Test
    public void testUnpack() throws Exception {
        File file = getTestFile();
        TelemetryUnpacker unpacker = new TelemetryUnpacker(this::decryptRSA);
        String path = file.getAbsolutePath();
        File unpacked = unpacker.unpack(path);
        Assert.assertEquals(path.substring(0, path.lastIndexOf('.')), unpacked.getAbsolutePath());
        // We have a directory here.
        final File[] files = unpacked.listFiles();
        Assert.assertNotNull(files);
        Assert.assertTrue(files.length > 0);
        // Now, list the topologies
        Arrays.stream(files)
              .forEach(topology -> {
                  int bitMask = 0;
                  try {
                      try (ZipInputStream in = new ZipInputStream(new FileInputStream(topology))) {
                          for (ZipEntry ze = in.getNextEntry(); ze != null;
                               ze = in.getNextEntry()) {
                              bitMask = calculateBitMask(bitMask, ze.getName());
                              // Test the entities.
                              if (ze.getName().startsWith("Entities")) {
                                  testEntities(in);
                              }
                              in.closeEntry();
                          }
                      }
                  } catch (IOException e) {
                      Assert.fail(e.getMessage());
                  }
                  // When we check for the presence of files, we use a bit-mask.
                  // So, if all 2 files are present and accounted for, we should have 1 | 2 = 3.
                  Assert.assertEquals(3, bitMask);
              });
    }

    /**
     * Tests the unpacking of the archive.
     */
    @Test(expected = IOException.class)
    public void testUnpackWrongKey() throws Exception {
        File file = getTestFile();
        TelemetryUnpacker unpacker = new TelemetryUnpacker(this::decryptRSAWrongKey);
        String path = file.getAbsolutePath();
        unpacker.unpack(path);
    }

    @Test(expected = IOException.class)
    public void badFile() throws IOException {
        TelemetryUnpacker unpacker = new TelemetryUnpacker(this::decryptRSA);
        unpacker.unpack("/dev/null");
    }

    /**
     * Tests the entities loading.
     *
     * @param in the Zip input stream.
     * @throws IOException In case of an error reading the entities.
     */
    private void testLoadEntities(final @Nonnull InputStream in) throws IOException {
        TelemetryParser parser = new TelemetryParser();
        TelemetryElasticLoader loader = new TelemetryElasticLoader("test");
        parser.parseEntities(in, loader.getEntitiesProcessor());
    }

    @Test(expected = ElasticLoadingException.class)
    public void testUploadError() throws ElasticLoadingException {
        TelemetryElasticLoader loader = new TelemetryElasticLoader("    ");
        loader.getEntitiesProcessor().apply("entry", 10L);
    }

    /**
     * Tests the unpacking of the archive.
     */
    @Test
    public void testUnpackAndLoad() throws Exception {
        File file = getTestFile();
        TelemetryUnpacker unpacker = new TelemetryUnpacker(this::decryptRSA);
        File unpacked = unpacker.unpack(file.getAbsolutePath());
        // We have a directory here.
        final File[] files = unpacked.listFiles();
        Assert.assertNotNull(files);
        // Now, list the topologies
        Arrays.stream(files)
              .forEach(topology -> {
                  try {
                      try (ZipInputStream in = new ZipInputStream(new FileInputStream(topology))) {
                          for (ZipEntry ze = in.getNextEntry(); ze != null;
                               ze = in.getNextEntry()) {
                              if (ze.getName().startsWith("Entities")) {
                                  testLoadEntities(in);
                              }
                              in.closeEntry();
                          }
                      }
                  } catch (IOException e) {
                      Assert.fail(e.getMessage());
                  }
              });
    }
}
