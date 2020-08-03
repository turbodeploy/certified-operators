package com.vmturbo.clustermgr.transfer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import com.google.common.base.Strings;

import com.vmturbo.clustermgr.aggregator.DataAggregator;

/**
 * The DataTransfer facilitates the encryption and transfer of the
 * customer telemetry data to the Turbonomic storage facility.
 * The current implementation is AWS specific.
 * The telemetry data is being collected by the {@link DataAggregator}, and stored in the local
 * file system. The data will then be first encrypted and then transferred.
 * The files in the local file system are kept only until they are transferred, or until
 * 2x collection interval passes, at which point the encrypted and archived data will be deleted.
 * The files are encrypted using AES/GCM with every entry using its own randomly generated key
 * and the nonce.
 * Reusing nonce with the the same encryption key is prohibited, since GCM mode turns AES into a
 * stream cipher. This means that if the nonce (IV) is reused, the plaintext data can be guessed at.
 * What that means is the fact that it is easier to just generate the key and the nonce for each
 * file to be added to the zip archive.
 * <p>
 * TODO(MB): Design a mechanism (probably by using the shared file system) to keep the locally
 */
public class DataTransfer {
    /**
     * The logger.
     */
    @VisibleForTesting
    static Logger logger_ = LogManager.getLogger(DataTransfer.class);

    /**
     * The base point for the data persistence.
     */
    @VisibleForTesting
    static String BASE_PATH = System.getProperty("datatransfer.base",
                                                 "/tmp/transfer");

    /**
     * The execution thread.
     */
    private Thread thread_;

    /**
     * The executor.
     */
    private final TransferExecutor executor_;

    /**
     * Constructs the data transfer.
     *
     * @param interval        The transfer interval.
     * @param dataAggregator  The data aggregator.
     * @param publicKey       The public key to encrypt session keys.
     * @param customerID      The customer id.
     * @param accessKey       The access key.
     * @param secretAccessKey The secret access key.
     * @param kvCollector     The key/value collector.
     */
    public DataTransfer(final long interval,
                        final @Nonnull DataAggregator dataAggregator,
                        final @Nonnull String publicKey,
                        final @Nonnull String customerID,
                        final @Nonnull String accessKey,
                        final @Nonnull String secretAccessKey,
                        final @Nonnull KVCollector kvCollector) {
        executor_ = new TransferExecutor(interval, dataAggregator, publicKey, customerID, accessKey,
                                         secretAccessKey, kvCollector);
    }

    /**
     * Starts the data transfer thread.
     */
    public void start() {
        thread_ = new Thread(executor_);
        thread_.setDaemon(true);
        thread_.start();
    }

    /**
     * Stops the data transfer thread.
     */
    public void stop() {
        thread_.interrupt();
    }

    /**
     * The transfer executor.
     */
    @VisibleForTesting static class TransferExecutor implements Runnable {
        /**
         * The symmetric cipher algorithm.
         */
        private static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";

        /**
         * The asymmetric cipher algorithm.
         */
        private static final String ASYM_CIPHER_ALGORITHM = "RSA";

        /**
         * The GCM tag length in bits.
         */
        private static final int GCM_TAG_LENGTH = 128;

        /**
         * The cipher key length in bytes. 128 bit.
         */
        @VisibleForTesting
        static int KEY_LENGTH_BYTES = 16;

        /**
         * The cipher key specification algorithm
         */
        private static final String KEYSPEC_ALGORITHM = "AES";

        /**
         * The copy data array. We use it to copy the encrypted data
         * into the ZIP archive.
         */
        private final byte[] data_ = new byte[32768];

        /**
         * The data transfer interval.
         */
        private final long interval_;

        /**
         * The Data aggregator.
         */
        private final DataAggregator dataAggregator_;

        /**
         * The public key.
         */
        @VisibleForTesting
        PublicKey publicKey_;

        /**
         * The multi-part part size. May be between 5MB and 5GB in size.
         * We settle at the 5MB default.
         */
        private static final long PART_SIZE = 0x500000;

        /**
         * The bucket name.
         */
        private static final String BUCKET_NAME = "turbonomic6";

        /**
         * The customer ID.
         */
        @VisibleForTesting
        String customerID_;

        /**
         * The access key.
         */
        @VisibleForTesting
        String accessKey_;

        /**
         * The secret access key.
         */
        @VisibleForTesting
        String secretAccessKey_;

        /**
         * The key/value store.
         */
        private KVCollector kvCollector_;

        /**
         * The global enabled flag.
         */
        private boolean enabled_;

        /**
         * Constructs the transfer executor.
         *
         * @param interval        The transfer interval.
         * @param dataAggregator  The data aggregator.
         * @param publicKey       The public key to encrypt session keys.
         * @param customerID      The customer id.
         * @param accessKey       The access key.
         * @param secretAccessKey The secret access key.
         * @param kvCollector     The key/value collector.
         */
        @VisibleForTesting TransferExecutor(final long interval,
                                            final @Nonnull DataAggregator dataAggregator,
                                            final @Nonnull String publicKey,
                                            final @Nonnull String customerID,
                                            final @Nonnull String accessKey,
                                            final @Nonnull String secretAccessKey,
                                            final @Nonnull KVCollector kvCollector) {
            interval_ = interval;
            dataAggregator_ = Objects.requireNonNull(dataAggregator);
            publicKey_ = decodePublicKey(publicKey);
            customerID_ = customerID;
            accessKey_ = accessKey;
            secretAccessKey_ = secretAccessKey;
            kvCollector_ = Objects.requireNonNull(kvCollector);
        }

        /**
         * Decodes the public key.
         *
         * @param publicKeyString The String representation of the encoded public key.
         * @return The decoded public key.
         */
        @VisibleForTesting
        static @Nonnull PublicKey decodePublicKey(final @Nonnull String publicKeyString) {
            try {
                byte[] rawKey = Base64.getDecoder().decode(publicKeyString.getBytes("UTF-8"));
                X509EncodedKeySpec keySpec = new X509EncodedKeySpec(rawKey);
                KeyFactory kf = KeyFactory.getInstance(ASYM_CIPHER_ALGORITHM);
                return kf.generatePublic(keySpec);
            } catch (UnsupportedEncodingException | NoSuchAlgorithmException |
                    InvalidKeySpecException e) {
                throw new SecurityException("Unable to deserialize the public key", e);
            }
        }

        /**
         * Checks whether LDCF is enabled.
         *
         * @return {@code true} iff enabled.
         */
        @VisibleForTesting
        boolean checkEnabled() {
            enabled_ = (kvCollector_ == null) ? enabled_ : kvCollector_.isEnabled();
            return enabled_;
        }

        /**
         * Implements the internal scheduler, which will encrypt and archive the current
         * datapoint. The datapoint represents a telemetry data collected from all the components
         * by the {@link DataAggregator} since the last data transfer (or the initial start of
         * Turbonomic).
         */
        @Override
        public void run() {
            final File dst = new File(BASE_PATH);
            for (; ; ) {
                // Wait for next cycle.
                try {
                    Thread.sleep(interval_);
                } catch (InterruptedException e) {
                    logger_.warn("The Data Transfer thread has been interrupted.");
                    break;
                }
                // Check whether we are enabled.
                // The side effects are:
                // 1. Thread that sleeps 10 minutes and checks the flag.
                // 2. The small overhead of Consul check. We have health checks already, so that's
                //    not a great burden.
                if (!checkEnabled()) {
                    continue;
                }
                // Create the directory if needed.
                if (!mkDir(dst)) {
                    continue;
                }
                cleanup(dst);
                executeCycle(dst);
            }
        }

        /**
         * Creates a directory.
         * Put into a separate method to facilitate the testing.
         *
         * @param dst The path for a directory to create.
         * @return {@code true} if successful, {@code false} otherwise.
         */
        @VisibleForTesting
        boolean mkDir(final @Nonnull File dst) {
            if (!dst.isDirectory() && !dst.mkdirs()) {
                logger_.error("Unable to create a directory: " + dst.getAbsolutePath());
                return false;
            }
            return true;
        }

        /**
         * Perform the cleanup of stale files.
         *
         * @param dst The destination directory.
         */
        @VisibleForTesting
        void cleanup(final @Nonnull File dst) {
            File[] files = dst.listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            // Clean up the old ones.
            long now = System.currentTimeMillis();
            long checkInterval = interval_ * 2;
            for (File file : files) {
                if (now - file.lastModified() > checkInterval) {
                    deleteFile(file);
                }
            }
        }

        /**
         * Executes a single cycle.
         * Will encrypt and compress the telemetry data, then will transfer it.
         * After that, the telemetry data staged by the {@link DataAggregator} will be deleted,
         * Since at that point, that telemetry data will be encrypted and archived by the Data Transfer.
         * The transfer is optional, since the customer might not have the internet access.
         *
         * @param dst The destination directory.
         */
        @VisibleForTesting
        void executeCycle(final @Nonnull File dst) {
            Optional<File> stagePoint = stageCheckpoint(dst);
            if (!stagePoint.isPresent()) {
                return;
            }
            try {
                if (Strings.isNullOrEmpty(customerID_) || Strings.isNullOrEmpty(accessKey_) ||
                    Strings.isNullOrEmpty(secretAccessKey_)) {
                    logger_.info("No upload information has been configured. " +
                                 " The file {} will have to be uploaded manually",
                                 stagePoint.get().getAbsolutePath());
                    return;
                }
                uploadFile(stagePoint.get());
            } catch (IOException e) {
                logger_.error(e.getMessage());
                return;
            }
            deleteFile(stagePoint.get());
        }

        /**
         * Obtain the AWS region.
         * <p>
         * The following is the order in which the region is obtained:
         * <ul>
         * <li>Environment variable {@code AWS_REGION}.</li>
         * <li>Java System property {@code aws.region}.</li>
         * <li>{@link Regions#US_EAST_1} as a default if all else fails.</li>
         * </ul>
         *
         * @return The region.
         * @throws IllegalArgumentException In case of an invalid region.
         */
        private static @Nonnull Regions getRegion() throws IllegalArgumentException {
            String region = System.getenv("AWS_REGION");
            region = Strings.isNullOrEmpty(region) ? System.getProperty("aws.region") : region;
            return Strings.isNullOrEmpty(region) ? Regions.US_EAST_1 :
                   Regions.fromName(region.toLowerCase());
        }

        /**
         * Returns the bucket name.
         *
         * @return The bucket name.
         */
        private @Nonnull String getBucketName() {
            return BUCKET_NAME;
        }

        /**
         * Returns the AWS credentials provider.
         * The AWS credentials provider will return {@link #accessKey_} and
         * {@link #secretAccessKey_}.
         *
         * @return The AWS credentials provider.
         */
        private @Nonnull AWSCredentialsProvider getAwsCredentialsProvider() {
            return new AWSStaticCredentialsProvider(new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return accessKey_;
                }

                @Override
                public String getAWSSecretKey() {
                    return secretAccessKey_;
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
                final String key = customerID_ + "_" + file.getName();
                final String bucketName = getBucketName();
                AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                                                   .withCredentials(getAwsCredentialsProvider())
                                                   .withRegion(getRegion()).build();
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
         * Stages the checkpoint.
         *
         * @param dst The destination directory.
         */
        @VisibleForTesting
        Optional<File> stageCheckpoint(final @Nonnull File dst) {
            final File checkPoint = dataAggregator_.startAggregationInterval();
            // Copy the file in an encrypted fashion.
            final File[] src = checkPoint.listFiles();
            if (src == null) {
                return Optional.empty();
            }
            // Copy all the files with encryption.
            final String fileName = dst.getAbsolutePath() + "/" +
                                    outputFilePrefix() + "_transfer.zip";
            try (FileOutputStream out = new FileOutputStream(fileName);
                 ZipOutputStream zip = new ZipOutputStream(out)) {
                // We compress compressed data, so the zip is really a container.
                zip.setLevel(0);
                for (File f : src) {
                    copyFile(f, zip);
                }
                // Delete the checkpoint directory.
                FileUtils.deleteDirectory(checkPoint);
            } catch (IOException | GeneralSecurityException e) {
                logger_.error("Unable to move files", e);
                deleteFile(new File(fileName));
                throw new SecurityException(e);
            }
            return Optional.of(new File(fileName));
        }

        /**
         * Returns the output file suffix.
         *
         * @return The file suffix.
         */
        private @Nonnull String outputFilePrefix() {
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss");
            dateFormat.setTimeZone(calendar.getTimeZone());
            return dateFormat.format(calendar.getTime());
        }

        /**
         * Encrypts the key and nonce and writes the result into the zip output stream.
         * The encrypted data will be prefixed with its length, since it is impossible
         * to predict the precise size of the encrypted data.
         *
         * @param rawKey The raw key (byte array).
         * @param nonce  The nonce.
         * @param out    The output stream.
         * @throws IOException              In case of an error performing write to the zip
         *                                  output stream.
         * @throws GeneralSecurityException In case of an error encrypting key or nonce.
         */
        private void encryptKeyAndNonce(final @Nonnull byte[] rawKey, final @Nonnull byte[] nonce,
                                        final @Nonnull DataOutputStream out)
                throws IOException, GeneralSecurityException {
            final Cipher cipherRSA = Cipher.getInstance(ASYM_CIPHER_ALGORITHM);
            cipherRSA.init(Cipher.ENCRYPT_MODE, publicKey_);
            // Encrypt and write the key and the nonce.
            ByteArrayOutputStream encrypted = new ByteArrayOutputStream(rawKey.length * 2);
            encrypted.write(cipherRSA.update(rawKey, 0, rawKey.length));
            encrypted.write(cipherRSA.update(nonce, 0, nonce.length));
            encrypted.write(cipherRSA.doFinal());
            // We use run length encoding there.
            out.writeInt(encrypted.size());
            out.write(encrypted.toByteArray());
        }

        /**
         * Create a byte array of the supplied size and fill with pseudo-random data.
         * We use strong PRNG, as defined in the the {@code securerandom.strongAlgorithms}
         * {@link java.security.Security} property.
         * We instantiate and seed the SecureRandom every time here.
         *
         * @param length The length.
         * @return The byte array of the {@code length} bytes filled with random data.
         * @throws NoSuchAlgorithmException In case of an error obtaing the SecureRandom instance.
         */
        private @Nonnull byte[] randomBytes(final int length) throws NoSuchAlgorithmException {
            SecureRandom rnd = new SecureRandom();
            final byte[] data = new byte[length];
            rnd.nextBytes(data);
            return data;
        }

        /**
         * Copies the file.
         *
         * @param src The source file to copy.
         * @param zip The destination archive.
         * @throws IOException In the case of an error encrypting the data.
         */
        private void copyFile(final @Nonnull File src, final @Nonnull ZipOutputStream zip)
                throws IOException, GeneralSecurityException {
            // Instantiate symmetric cipher
            final Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            // Generate symmetric session key.
            final byte[] rawKey = randomBytes(KEY_LENGTH_BYTES);
            final byte[] nonce = randomBytes(cipher.getBlockSize());
            final SecretKey keySpec = new SecretKeySpec(rawKey, KEYSPEC_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH, nonce));
            // Instantiate asymmetric cipher.
            final Cipher cipherRSA = Cipher.getInstance(ASYM_CIPHER_ALGORITHM);
            cipherRSA.init(Cipher.ENCRYPT_MODE, publicKey_);
            // Store the data
            zip.putNextEntry(new ZipEntry(src.getName()));
            try (FileInputStream in = new FileInputStream(src)) {
                // Store encrypted key and nonce prefixed by its length.
                encryptKeyAndNonce(rawKey, nonce, new DataOutputStream(zip));
                for (int cbRead = in.read(data_); cbRead > 0; cbRead = in.read(data_)) {
                    zip.write(cipher.update(data_, 0, cbRead));
                }
                zip.write(cipher.doFinal());
            } finally {
                // We want the data copy exception to be shown in logs, since that will provide
                // us with more relevant information.
                closeZipEntry(zip);
            }
        }

        /**
         * We have a separate method, so that we can test functionality.
         *
         * @param zip The zip output stream.
         */
        @VisibleForTesting
        void closeZipEntry(final @Nonnull ZipOutputStream zip) {
            try {
                zip.closeEntry();
            } catch (IOException eClose) {
                logger_.error("Error closing zip entry", eClose);
            }
        }

        /**
         * Deletes a file or an empty directory.
         *
         * @param path The path to delete.
         */
        private void deleteFile(final @Nonnull File path) {
            if (!path.delete()) {
                logger_.error("Error deleting file: " + path.getAbsolutePath());
            }
        }
    }

    /**
     * Checks whether telemetry is enabled.
     */
    public interface KVCollector {
        boolean isEnabled();
    }
}
