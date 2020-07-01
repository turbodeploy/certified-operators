package com.turbonomic.telemetry;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * The {@link TelemetryRetrieverTestIT} implements tests for telemetry retrieval.
 */
public class TelemetryRetrieverTestIT {
    private static final String CUSTOMER_ID = "Turbonomic";
    private static final String ACCESS_KEY = "AKIAIQ7RIJNU6CJ4W6ZQ";
    private static final String SECRET_KEY = "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";
    private static final long PART_SIZE = 0x500000;
    private static final String BUCKET_NAME = "turbonomic6";

    private @Nonnull AWSCredentialsProvider getAwsCredentialsProvider() {
        return new AWSStaticCredentialsProvider(new AWSCredentials() {
            @Override
            public String getAWSAccessKeyId() {
                return ACCESS_KEY;
            }

            @Override
            public String getAWSSecretKey() {
                return SECRET_KEY;
            }
        });
    }

    /**
     * Uploads the file to Amazon S3.
     *
     * @param file The file to upload.
     * @throws IOException In the case of an error uploading the file.
     */
    @VisibleForTesting
    void uploadFile(final @Nonnull File file) throws IOException {
        try {
            // We use folder-like key structure.
            // Amazon S3 recognizes folder/file structure and presents files as such in CLI.
            // And it also makes it easier to group the files uploaded by an individual
            // customer.
            final String key = CUSTOMER_ID + "_" + file.getName();
            final String bucketName = BUCKET_NAME;
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                                               .withCredentials(getAwsCredentialsProvider())
                                               .withRegion(Regions.US_EAST_1).build();
            List<PartETag> partETags = new ArrayList<>();
            InitiateMultipartUploadRequest initReq;
            initReq = new InitiateMultipartUploadRequest(bucketName, key);
            InitiateMultipartUploadResult initResp = s3.initiateMultipartUpload(initReq);
            String uploadID = initResp.getUploadId();
            try {
                final long size = file.length();
                long pos = 0L;
                for (int i = 1; pos < size; i++) {
                    final long partSize = Math.min(PART_SIZE, (size - pos));
                    UploadPartRequest uploadRequest = new UploadPartRequest()
                            .withBucketName(bucketName).withKey(key).withUploadId(uploadID)
                            .withPartNumber(i).withFileOffset(pos).withFile(file)
                            .withPartSize(partSize);
                    // Upload part and add response to our list.
                    partETags.add(s3.uploadPart(uploadRequest).getPartETag());
                    pos += partSize;
                }
                CompleteMultipartUploadRequest compRequest;
                compRequest = new CompleteMultipartUploadRequest(bucketName, key, uploadID,
                                                                 partETags);
                s3.completeMultipartUpload(compRequest);
            } catch (Exception e) {
                s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key,
                                                                        uploadID));
                throw new IOException("Error uploading data", e);
            }
        } catch (AmazonClientException | IllegalArgumentException e) {
            throw new IOException("Error initiating file upload", e);
        }
    }

    /**
     * Obtains the SHA-256 digest of the file.
     *
     * @param file The file.
     * @return The SHA-256 digest.
     * @throws IOException In case of an error generating the digest.
     */
    private byte[] digest(final @Nonnull File file) throws IOException {
        try (InputStream in = new FileInputStream(file)) {
            return DigestUtils.sha256(in);
        }
    }

    /**
     * Tests the unpacking of the archive.
     */
    @Test
    public void testRetrive() throws Exception {
        File file = TelemetryUnpackerTestIT.getTestFile();
        TelemetryRetriever telemetryRetriever = new TelemetryRetriever(SECRET_KEY, ACCESS_KEY,
                                                                       Regions.US_EAST_1);
        uploadFile(file);
        String s3Key = "Turbonomic_telemetry_test.zip";
        Assert.assertTrue(telemetryRetriever.listFiles().contains(s3Key));
        telemetryRetriever.retrieveFile(s3Key, "/tmp/test.zip");
        File s3Copy = new File("/tmp/test.zip");
        // Obtain and compare SHA-256 digests for the local and uploaded/downloaded copy.
        Assert.assertArrayEquals(digest(file), digest(s3Copy));
        s3Copy.delete();
        telemetryRetriever.cleanUpBucket(Collections.singletonList(s3Key));
        Assert.assertFalse(telemetryRetriever.listFiles().contains(file.getName()));
    }

    /**
     * Tests the unpacking of the archive.
     */
    @Test(expected = IOException.class)
    public void testBadRetrive() throws Exception {
        TelemetryRetriever telemetryRetriever = new TelemetryRetriever(SECRET_KEY, ACCESS_KEY,
                                                                       Regions.US_WEST_1);
        String s3Key = "Turbonomic_telemetry_test.zip";
        telemetryRetriever.retrieveFile(s3Key, "/tmp/test.zip");
    }

    @Test(expected = IOException.class)
    public void testS3ClientException() throws IOException {
        TelemetryRetriever mock = Mockito.spy(new TelemetryRetriever(SECRET_KEY,
                                                                     ACCESS_KEY,
                                                                     Regions.US_WEST_1));
        AmazonS3 s3Mock = Mockito.mock(AmazonS3.class);
        Mockito.when(s3Mock.getObject(Mockito.any())).thenThrow(new AmazonClientException("Test"));
        Mockito.when(mock.getS3client()).thenReturn(s3Mock);
        String s3Key = "Turbonomic_telemetry_test.zip";
        mock.retrieveFile(s3Key, "/tmp/test.zip");
    }

    @Test
    public void testEmptyCleanup() {
        TelemetryRetriever telemetryRetriever = new TelemetryRetriever(SECRET_KEY, ACCESS_KEY,
                                                                       Regions.US_WEST_1);
        telemetryRetriever.cleanUpBucket(Collections.emptyList());
    }
}
