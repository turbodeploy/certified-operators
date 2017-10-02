package com.vmturbo.clustermgr.transfer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.vmturbo.clustermgr.aggregator.DataAggregator;
import com.vmturbo.clustermgr.transfer.DataTransfer.TransferExecutor;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * The DataTransferTest implements DataTransfer tests.
 */
public class DataTransferTestIT {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private String pathSrc;
    private File pathDst;

    private static final String FILE_DATA_1 = "Data in file 1";
    private static final String FILE_DATA_2 = "Data in file 2";

    private static final String CUSTOMER_ID_INIT = "Cisco";
    private static String CUSTOMER_ID;
    private static final String ACCESS_KEY = "AKIAIQ7RIJNU6CJ4W6ZQ";
    private static final String SECRET_KEY = "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";


    /**
     * Generate the Base64-encoded SHA-1 digest of the str.
     * The result will be massaged to allow it to be a part of the file name.
     *
     * @param str The string to be digested.
     * @return The massaged Base64-encoded SHA-1 digest of the str.
     * @throws NoSuchAlgorithmException     In case SHA-1 is unsupported.
     * @throws UnsupportedEncodingException In case UTF-8 is unsupported.
     */
    private @Nonnull String digest(final @Nonnull String str)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] digest = md.digest(str.getBytes("UTF-8"));
        return new String(Base64.getEncoder().encode(digest), "UTF-8")
                .replace('/', '.')
                .replace('+', '-')
                .replace('=', '_');
    }

    @Before
    public void setup() throws Exception {
        pathSrc = tempFolder.newFolder().getCanonicalPath();
        pathDst = new File(tempFolder.newFolder().getCanonicalPath());
        Path f1 = Files.createFile(Paths.get(pathSrc + "/file1"));
        Path f2 = Files.createFile(Paths.get(pathSrc + "/file2"));
        try (FileOutputStream out = new FileOutputStream(f1.toFile())) {
            out.write(FILE_DATA_1.getBytes());
        }
        try (FileOutputStream out = new FileOutputStream(f2.toFile())) {
            out.write(FILE_DATA_2.getBytes());
        }
        DataTransfer.BASE_PATH = pathDst.getAbsolutePath();
        TransferExecutor.KEY_LENGTH_BYTES = 16;
        System.setProperty("aws.region", Regions.US_EAST_1.getName());
        DataTransfer.logger_ = LogManager.getLogger(DataTransfer.class);

        CUSTOMER_ID = digest(CUSTOMER_ID_INIT) + "_" + digest("TestInstance");
    }

    /**
     * Encodes the public key.
     *
     * @param publicKey The public key.
     * @return The public key encoded as Base64 UTF-8 string.
     */
    private String encodePublicKey(final @Nonnull PublicKey publicKey) {
        try {
            return new String(Base64.getEncoder().encode(publicKey.getEncoded()), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 is unsupported", e);
        }
    }

    /**
     * Generates the RSA 3K key pair.
     *
     * @return The key pair.
     * @throws NoSuchAlgorithmException In case RSA is not available.
     */
    private KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(3072, SecureRandom.getInstanceStrong());
        return keyGen.generateKeyPair();
    }

    /**
     * Tests the public key serialization/deserialization.
     */
    @Test
    public void testKeySerialization() throws Exception {
        KeyPair keyPair = generateKeyPair();
        final String data = "DATA";

        // Do the first round - use the keys as is.
        Cipher cipherRSA = Cipher.getInstance("RSA");
        cipherRSA.init(Cipher.ENCRYPT_MODE, keyPair.getPublic());
        byte[] cipherText = cipherRSA.doFinal(data.getBytes());
        cipherRSA.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
        Assert.assertEquals(data, new String(cipherRSA.doFinal(cipherText)));

        // Serialize and deserialize the keys.
        cipherRSA = Cipher.getInstance("RSA");
        String encodedKey = encodePublicKey(keyPair.getPublic());
        PublicKey publicKey = TransferExecutor.decodePublicKey(encodedKey);
        cipherRSA.init(Cipher.ENCRYPT_MODE, publicKey);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(cipherRSA.update(data.getBytes()));
        out.write(cipherRSA.doFinal());
        out.flush();
        cipherText = out.toByteArray();
        cipherRSA.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
        Assert.assertEquals(data, new String(cipherRSA.doFinal(cipherText)));
    }

    /**
     * Tests the public key serialization/deserialization.
     */
    @Test(expected = SecurityException.class)
    public void testKeySerializationFailure() throws Exception {
        TransferExecutor.decodePublicKey("hahaha");
    }

    /**
     * Retrieves the data from the zip and validates the zip entry name.
     *
     * @param in   The zip input stream.
     * @param name The name to be valiadated.
     * @return The extracted data.
     * @throws Exception In case of any error.
     */
    private byte[] getDataFromZip(final @Nonnull ZipInputStream in, final @Nonnull String name)
            throws Exception {
        ZipEntry ze = in.getNextEntry();
        Assert.assertEquals(name, ze.getName());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
        in.closeEntry();
        return out.toByteArray();
    }

    private byte[] getRawFileData(final @Nonnull byte[] in, final @Nonnull KeyPair keyPair)
            throws Exception {
        Cipher cipherRSA = Cipher.getInstance("RSA");
        cipherRSA.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(in));
        int length = dis.readInt();
        byte[] keyAndNonce = new byte[length];
        dis.readFully(keyAndNonce);
        byte[] raw = cipherRSA.doFinal(keyAndNonce);
        byte[] rawKey = new byte[16];
        byte[] nonce = new byte[16];
        System.arraycopy(raw, 0, rawKey, 0, rawKey.length);
        System.arraycopy(raw, rawKey.length, nonce, 0, nonce.length);

        byte[] fileData = new byte[dis.available()];
        dis.readFully(fileData);
        SecretKey keySpec = new SecretKeySpec(rawKey, "AES");
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, keySpec, new GCMParameterSpec(128, nonce));
        return cipher.doFinal(fileData);
    }

    /**
     * Tests the checkpoint staging.
     */
    @Test
    public void testStageCheckpoint() throws Exception {
        KeyPair keyPair = generateKeyPair();
        DataAggregator dataAggregator = Mockito.mock(DataAggregator.class);
        when(dataAggregator.startAggregationInterval()).thenReturn(new File(pathSrc));
        String szPublicKey = encodePublicKey(keyPair.getPublic());
        TransferExecutor executor = new TransferExecutor(0L,
                                                         dataAggregator,
                                                         szPublicKey, CUSTOMER_ID, ACCESS_KEY,
                                                         SECRET_KEY, () -> {
            return true;
        });
        executor.stageCheckpoint(pathDst);
        File[] files = pathDst.listFiles();
        Assert.assertNotNull(files);
        try (FileInputStream fin = new FileInputStream(files[0]);
             ZipInputStream in = new ZipInputStream(fin)) {
            byte[] file1 = getDataFromZip(in, "file1");
            byte[] file2 = getDataFromZip(in, "file2");
            Assert.assertEquals(FILE_DATA_1, new String(getRawFileData(file1, keyPair)));
            Assert.assertEquals(FILE_DATA_2, new String(getRawFileData(file2, keyPair)));
        }
    }

    /**
     * Deletes all objects in the bucket.
     */
    private void cleanUpBucket(List<String> list) throws Exception {
        if (list.isEmpty()) {
            return;
        }
        // Check the files
        AWSCredentialsProvider awsCredentialsProvider = new AWSCredentialsProviderChain(
                new AWSStaticCredentialsProvider(new AWSCredentials() {
                    @Override public String getAWSAccessKeyId() {
                        return ACCESS_KEY;
                    }

                    @Override public String getAWSSecretKey() {
                        return SECRET_KEY;
                    }
                })
        );
        AmazonS3 s3client = AmazonS3ClientBuilder.standard()
                                                 .withCredentials(awsCredentialsProvider)
                                                 .withRegion(Regions.US_EAST_1)
                                                 .build();
        List<DeleteObjectsRequest.KeyVersion> keys = list.stream()
                                                         .map(DeleteObjectsRequest.KeyVersion::new)
                                                         .collect(Collectors.toList());
        // Delete all the files.
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest("turbonomic6");
        deleteObjectsRequest.setKeys(keys);
        s3client.deleteObjects(deleteObjectsRequest);
    }

    private List<String> getFilesInBucket() throws Exception {
        AWSCredentialsProvider awsCredentialsProvider = new AWSCredentialsProviderChain(
                new AWSStaticCredentialsProvider(new AWSCredentials() {
                    @Override public String getAWSAccessKeyId() {
                        return ACCESS_KEY;
                    }

                    @Override public String getAWSSecretKey() {
                        return SECRET_KEY;
                    }
                })
        );
        AmazonS3 s3client = AmazonS3ClientBuilder.standard()
                                                 .withCredentials(awsCredentialsProvider)
                                                 .withRegion(Regions.US_EAST_1)
                                                 .build();
        final ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName("turbonomic6")
                .withMaxKeys(10);
        ListObjectsV2Result result;
        List<String> keys = new ArrayList<>();
        do {
            result = s3client.listObjectsV2(req);
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                keys.add(objectSummary.getKey());
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return keys;
    }

    /**
     * Tests the checkpoint staging.
     */
    @Test
    public void testStageCheckpointAndUpload() throws Exception {
        cleanUpBucket(getFilesInBucket());
        KeyPair keyPair = generateKeyPair();
        DataAggregator dataAggregator = Mockito.mock(DataAggregator.class);
        when(dataAggregator.startAggregationInterval()).thenReturn(new File(pathSrc));
        String szPublicKey = encodePublicKey(keyPair.getPublic());
        TransferExecutor executor = new TransferExecutor(0L,
                                                         dataAggregator,
                                                         szPublicKey, CUSTOMER_ID, ACCESS_KEY,
                                                         SECRET_KEY, () -> {
            return true;
        });
        executor.stageCheckpoint(pathDst);
        File[] files = pathDst.listFiles();
        Assert.assertNotNull(files);
        try (FileInputStream fin = new FileInputStream(files[0]);
             ZipInputStream in = new ZipInputStream(fin)) {
            byte[] file1 = getDataFromZip(in, "file1");
            byte[] file2 = getDataFromZip(in, "file2");
            Assert.assertEquals(FILE_DATA_1, new String(getRawFileData(file1, keyPair)));
            Assert.assertEquals(FILE_DATA_2, new String(getRawFileData(file2, keyPair)));
        }
        executor.uploadFile(files[0]);
        List<String> list = getFilesInBucket();
        // Check the files
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(CUSTOMER_ID + "_" + files[0].getName(), list.get(0));
        cleanUpBucket(getFilesInBucket());
    }

    private TransferExecutor getTransferExecutor(final long interval,
                                                 final @Nonnull KeyPair keyPair,
                                                 final @Nonnull DataAggregator dataAggregator) {
        String szPublicKey = encodePublicKey(keyPair.getPublic());
        return Mockito
                .spy(new TransferExecutor(interval, dataAggregator, szPublicKey, CUSTOMER_ID,
                                          ACCESS_KEY, SECRET_KEY, () -> {
                    return true;
                }));
    }

    private TransferExecutor getDefaultTransferExecutor(final long interval) throws Exception {
        KeyPair keyPair = generateKeyPair();
        DataAggregator dataAggregator = Mockito.mock(DataAggregator.class);
        when(dataAggregator.startAggregationInterval()).thenReturn(new File(pathSrc));
        return getTransferExecutor(interval, keyPair, dataAggregator);
    }

    /**
     * Tests the upload of a 8MB file.
     */
    @Test
    public void testUploadLargeFile() throws Exception {
        cleanUpBucket(getFilesInBucket());
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        File file = Files.createFile(Paths.get(pathSrc + "/large")).toFile();
        try (FileOutputStream out = new FileOutputStream(file)) {
            byte[] data = new byte[0x800000];
            for (int i = 0; i < 16; i++) {
                data[i] = (byte)(i & 0xFF);
            }
            out.write(data);
        }
        executor.uploadFile(file);
        List<String> list = getFilesInBucket();
        // Check the files
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(CUSTOMER_ID + "_" + file.getName(), list.get(0));
        cleanUpBucket(getFilesInBucket());
    }

    /**
     * Tests the edge case when the upload fails because of the read issues.
     */
    @Test(expected = IOException.class)
    public void testUploadFailOnRead() throws Exception {
        cleanUpBucket(getFilesInBucket());
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        File file = Files.createFile(Paths.get(pathSrc + "/no_read")).toFile();
        Assert.assertTrue(file.setReadable(false));
        executor.uploadFile(file);
        cleanUpBucket(getFilesInBucket());
    }

    /**
     * Tests the upload of a single file.
     */
    @Test
    public void testCycle() throws Exception {
        cleanUpBucket(getFilesInBucket());
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        executor.executeCycle(pathDst);
        File[] files = pathDst.listFiles();
        Assert.assertNotNull(files);
        List<String> list = getFilesInBucket();
        // Check the files
        // Checks
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(0, files.length);
        cleanUpBucket(getFilesInBucket());
    }

    /**
     * Tests the empty AWS region defined by Java property.
     */
    @Test
    public void testEmptyJavaPropertyRegion() throws Exception {
        System.setProperty("aws.region", "");
        cleanUpBucket(getFilesInBucket());
        KeyPair keyPair = generateKeyPair();
        DataAggregator dataAggregator = Mockito.mock(DataAggregator.class);
        when(dataAggregator.startAggregationInterval()).thenReturn(new File(pathSrc));
        TransferExecutor executor = getTransferExecutor(0L, keyPair, dataAggregator);
        executor.stageCheckpoint(pathDst);
        File[] files = pathDst.listFiles();
        Assert.assertNotNull(files);
        try (FileInputStream fin = new FileInputStream(files[0]);
             ZipInputStream in = new ZipInputStream(fin)) {
            byte[] file1 = getDataFromZip(in, "file1");
            byte[] file2 = getDataFromZip(in, "file2");
            Assert.assertEquals(FILE_DATA_1, new String(getRawFileData(file1, keyPair)));
            Assert.assertEquals(FILE_DATA_2, new String(getRawFileData(file2, keyPair)));
        }
        executor.uploadFile(files[0]);
        List<String> list = getFilesInBucket();
        // Check the files
        // Checks
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(CUSTOMER_ID + "_" + files[0].getName(), list.get(0));
        cleanUpBucket(getFilesInBucket());
    }

    /**
     * Tests the failed upload due to the unreadable file.
     */
    @Test(expected = IOException.class)
    public void testStageCheckpointAndUploadBadRegion() throws Exception {
        System.setProperty("aws.region", "NoSuchRegionEver!!!");
        cleanUpBucket(getFilesInBucket());
        KeyPair keyPair = generateKeyPair();
        DataAggregator dataAggregator = Mockito.mock(DataAggregator.class);
        when(dataAggregator.startAggregationInterval()).thenReturn(new File(pathSrc));
        String szPublicKey = encodePublicKey(keyPair.getPublic());
        TransferExecutor executor = Mockito.spy(new TransferExecutor(0L, dataAggregator,
                                                                     szPublicKey, CUSTOMER_ID,
                                                                     ACCESS_KEY, SECRET_KEY, () -> {
            return true;
        }));
        executor.stageCheckpoint(pathDst);
        File[] files = pathDst.listFiles();
        Assert.assertNotNull(files);
        try (FileInputStream fin = new FileInputStream(files[0]);
             ZipInputStream in = new ZipInputStream(fin)) {
            byte[] file1 = getDataFromZip(in, "file1");
            byte[] file2 = getDataFromZip(in, "file2");
            Assert.assertEquals(FILE_DATA_1, new String(getRawFileData(file1, keyPair)));
            Assert.assertEquals(FILE_DATA_2, new String(getRawFileData(file2, keyPair)));
        }
        executor.uploadFile(files[0]);
    }

    /**
     * We test bad base path.
     * The results are to be observed in the console.
     */
    @Test
    public void testErrorWithDir() throws Exception {
        DataTransfer.BASE_PATH = "/dev/null";
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        executor.executeCycle(pathDst);
    }

    /**
     * We test bad base path.
     * The results are to be observed in the console.
     */
    @Test
    public void testErrorWithMkDir() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        Mockito.doReturn(false).when(executor).mkDir(Mockito.any());
        Thread thread = new Thread(executor);
        thread.start();
        Thread.sleep(1000L);
        thread.interrupt();
        thread.join();
        verify(executor, never()).executeCycle(any());
    }

    /**
     * We test bad base path.
     * The results are to be observed in the console.
     */
    @Test
    public void testErrorWithMkDirFalse() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        File mockedFile = Mockito.mock(File.class);
        Mockito.when(mockedFile.isDirectory()).thenReturn(false);
        Mockito.when(mockedFile.mkdirs()).thenReturn(false);
        Assert.assertFalse(executor.mkDir(mockedFile));
    }

    private File[] checkNonemptyFileList(File dir) {
        File[] list = dir.listFiles();
        Assert.assertNotNull(list);
        return list;
    }

    /**
     * Test successful cleanup.
     */
    @Test
    public void testCleanup() throws Exception {
        File src = new File(pathSrc);
        TransferExecutor executor = getDefaultTransferExecutor(1000L);
        // Wait for 3 seconds
        Thread.sleep(3000L);
        Assert.assertEquals(2, checkNonemptyFileList(src).length);
        executor.cleanup(src);
        Assert.assertEquals(0, checkNonemptyFileList(src).length);
    }

    /**
     * Test the situation when files did not yet expire.
     */
    @Test
    public void testCleanupNoOp() throws Exception {
        File src = new File(pathSrc);
        TransferExecutor executor = getDefaultTransferExecutor(10000L);
        // Wait for 3 seconds
        Thread.sleep(500L);
        Assert.assertEquals(2, checkNonemptyFileList(src).length);
        executor.cleanup(src);
        Assert.assertEquals(2, checkNonemptyFileList(src).length);
    }

    @Test
    public void testBadCheckpoint() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        Mockito.doReturn(Optional.empty()).when(executor)
               .stageCheckpoint(pathDst);
        executor.executeCycle(pathDst);
        verify(executor, never()).uploadFile(any());
    }

    @Test
    public void testBadUpload() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        Mockito.doThrow(new IOException()).when(executor)
               .uploadFile(Mockito.any());
        executor.executeCycle(pathDst);
        verify(executor, Mockito.times(1)).uploadFile(any());
    }

    /**
     * Tests the checkpoint staging.
     */
    @Test
    public void testExecuteCycleWithThread() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        Mockito.doThrow(new RuntimeException()).when(executor).executeCycle(pathDst);
        Thread thread = new Thread(executor);
        thread.start();
        thread.join();
        verify(executor, Mockito.times(1)).executeCycle(pathDst);
    }

    /**
     * Tests the checkpoint staging.
     */
    @Test
    public void testStageCheckpointWithThreadInterrupted() throws Exception {
        KeyPair keyPair = generateKeyPair();
        DataAggregator dataAggregator = Mockito.mock(DataAggregator.class);
        when(dataAggregator.getCurrentBase()).thenReturn(new File(pathSrc));
        String szPublicKey = encodePublicKey(keyPair.getPublic());
        DataTransfer executor = new DataTransfer(6000000L, dataAggregator, szPublicKey,
                                                 CUSTOMER_ID, ACCESS_KEY, SECRET_KEY, () -> {
            return true;
        });
        executor.start();
        Thread.sleep(100L);
        executor.stop();
    }

    /**
     * Tests the disabled execution.
     */
    @Test
    public void testDisabled() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(10L);
        doReturn(false).when(executor).checkEnabled();
        Thread thread = new Thread(executor);
        thread.setDaemon(true);
        thread.start();
        Thread.sleep(2000L);
        thread.interrupt();
        thread.join();
        verify(executor, never()).executeCycle(any());
    }

    /**
     * Tests the checkpoint staging.
     * Will immediately return.
     */
    @Test
    public void testStageCheckpointNoOp() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        FileUtils.deleteDirectory(new File(pathSrc));
        FileUtils.deleteDirectory(pathDst);
        Assert.assertFalse(executor.stageCheckpoint(pathDst).isPresent());
    }

    /**
     * Tests the checkpoint staging.
     * Will immediately return.
     */
    @Test(expected = SecurityException.class)
    public void testStageCheckpointFailure() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        FileUtils.deleteDirectory(pathDst);
        executor.stageCheckpoint(pathDst);
    }

    /**
     * Tests the checkpoint staging.
     * Will immediately return.
     */
    @Test(expected = SecurityException.class)
    public void testStageCheckpointFailure2() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        File[] files = (new File(pathSrc)).listFiles();
        Assert.assertNotNull(files);
        Assert.assertTrue(files[0].setReadable(false));
        Assert.assertTrue(files[1].setReadable(false));
        executor.stageCheckpoint(pathDst);
    }

    @Test(expected = SecurityException.class)
    public void testCipherInitFailure() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        TransferExecutor.KEY_LENGTH_BYTES = 1024;
        executor.stageCheckpoint(pathDst);
    }

    /**
     * The results will show as an error message.
     * This tests a really edge case.
     */
    @Test(expected = SecurityException.class)
    public void testAsymmetricCipherInitFailure() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        executor.publicKey_ = null;
        executor.stageCheckpoint(pathDst);
    }

    /**
     * Tests the abscense of upload credentials.
     */
    @Test
    public void testNoUploadCreds() throws Exception {
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        executor.customerID_ = null;
        executor.executeCycle(pathDst);
        verify(executor, never()).uploadFile(any());
    }

    @Test
    public void testFailedZipClose() throws Exception {
        DataTransfer.logger_ = Mockito.spy(LogManager.getLogger(DataTransfer.class));
        TransferExecutor executor = getDefaultTransferExecutor(0L);
        ZipOutputStream zip = Mockito.mock(ZipOutputStream.class);
        doThrow(IOException.class).when(zip).closeEntry();
        executor.closeZipEntry(zip);
        verify(DataTransfer.logger_, times(1)).error(any(String.class), any(IOException.class));
    }

    @Test
    @Ignore
    public void testHugeZip() throws Exception {
        byte[] data = new byte[0x100000]; // 1MB
        SecureRandom.getInstanceStrong().nextBytes(data);
        File f = new File(pathSrc + "/test_huge.zip");
        try (FileOutputStream out = new FileOutputStream(f);
             ZipOutputStream zip = new ZipOutputStream(out)) {
            zip.setLevel(0);
            zip.putNextEntry(new ZipEntry("TheTest"));
            // Write 1MB 32K times, resulting in 32GB entry.
            for (int i = 0; i < 0x2000; i++) {
                zip.write(data, 0, data.length);
            }
            zip.closeEntry();
        }
        Assert.assertTrue(f.exists());
        Assert.assertTrue(f.length() >= 0x200000000L);
    }
}
