package com.turbonomic.telemetry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;

/**
 * The {@link TelemetryRetriever} implements the retrieval of the telemetry files.
 */
public class TelemetryRetriever {
    /**
     * The bucket
     */
    private static final String BUCKET = "turbonomic6";

    /**
     * The Amazon S3 secret key.
     */
    private final String secretKey_;

    /**
     * The Amazon S3 access key.
     */
    private final String accessKey_;

    /**
     * The target region.
     */
    private final Regions region_;

    /**
     * The AWS S3 credentials provider;
     */
    private final AWSCredentialsProvider awsCredentialsProvider_;

    /**
     * Constructs the retriever.
     *
     * @param secretKey The Amazon S3 secret key.
     * @param accessKey The Amazon S3 access key.
     * @param region    The Amazon region.
     */
    public TelemetryRetriever(final @Nonnull String secretKey, final @Nonnull String accessKey,
                              final @Nonnull Regions region) {
        secretKey_ = secretKey;
        accessKey_ = accessKey;
        region_ = region;
        awsCredentialsProvider_ = new AWSCredentialsProviderChain(
                new AWSStaticCredentialsProvider(new AWSCredentials() {
                    @Override public String getAWSAccessKeyId() {
                        return accessKey_;
                    }

                    @Override public String getAWSSecretKey() {
                        return secretKey_;
                    }
                })
        );
    }

    /**
     * Creates an instance of the Amazon S3 client.
     *
     * @return The Amazon S3 client.
     */
    @VisibleForTesting
    @Nonnull AmazonS3 getS3client() {
        return AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider_)
                                    .withRegion(region_).build();
    }

    /**
     * Retrieves the file.
     *
     * @param s3Key    The Amazon S3 bucket file name.
     * @param fileName The file name to store to.
     * @throws IOException In the case of an error downloading the file.
     */
    public void retrieveFile(final @Nonnull String s3Key, final @Nonnull String fileName)
            throws IOException {
        AmazonS3 s3client = getS3client();
        try {
            final @Nonnull GetObjectRequest objectRequest = new GetObjectRequest(BUCKET, s3Key);
            final @Nonnull S3Object s3object = s3client.getObject(objectRequest);
            try (InputStream in = s3object.getObjectContent();
                 FileOutputStream out = new FileOutputStream(fileName)) {
                IOUtils.copy(in, out);
            }
        } catch (AmazonServiceException e) {
            throw new IOException("The retrieval request has been rejected", e);
        } catch (AmazonClientException e) {
            throw new IOException("The retrieval request encountered an internal error", e);
        }
    }

    /**
     * Deletes all objects in the bucket.
     *
     * @param list The list of objects to be deleted from the Amazon S3 bucket.
     */
    public void cleanUpBucket(final @Nonnull List<String> list) {
        if (list.isEmpty()) {
            return;
        }
        List<DeleteObjectsRequest.KeyVersion> keys = list.stream()
                                                         .map(DeleteObjectsRequest.KeyVersion::new)
                                                         .collect(Collectors.toList());
        // Delete all the files.
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(BUCKET);
        deleteObjectsRequest.setKeys(keys);
        getS3client().deleteObjects(deleteObjectsRequest);
    }

    /**
     * Retrieves the list of files in the bucket.
     *
     * @return The list of files.
     */
    public @Nonnull List<String> listFiles() {
        // Maximum keys per request.
        final int MAX_KEYS_PER_REQUEST = 10;
        final @Nonnull AmazonS3 s3client = getS3client();
        final ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(BUCKET)
                .withMaxKeys(MAX_KEYS_PER_REQUEST);
        ListObjectsV2Result result;
        List<String> keys = new ArrayList<>();
        do {
            result = s3client.listObjectsV2(req);
            result.getObjectSummaries().stream().map(S3ObjectSummary::getKey)
                  .collect(Collectors.toCollection(() -> keys));
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return keys;
    }
}
