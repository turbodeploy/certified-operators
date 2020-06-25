package com.vmturbo.api.component.external.api.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.util.encoders.Base64;

import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.serviceinterfaces.IClassicMigrationService;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Service Layer to implement the /classic endpoints.
 **/
public class ClassicMigrationService implements IClassicMigrationService {

    private static final String CLASSIC_CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";

    static final String CHARSET_CRYPTO = "UTF-8";

    private final TargetsService targetsService;

    private static final int PKCS5_SALT_LENGTH = 0x20;

    private static final String MAGIC_COOKIE_V1 = "$_1_$VMT$$";

    private static final int CLASSIC_INSTANCE_KEY_LENGTH = 0x20;

    private static final int PBKDF2_ITERATIONS = 32768;

    private static final String KEYSPEC_ALGORITHM = "AES";

    private static final String CLASSIC_PBKDF2_DERIVATION_ALGORITHM = "PBKDF2WithHmacSHA1";

    private static final int KEY_LENGTH = 128;


    /**
     * Service to convert targets with classic encrypted password into xl targets.
     *
     * @param targetsService service to add the target
     */
    public ClassicMigrationService(@Nonnull TargetsService targetsService) {
        this.targetsService = Objects.requireNonNull(targetsService);
    }

    /**
     * Add a target from classic. In addition to the common field required for targets, this
     * method needs the encrypted password from the classic instance and the corresponding
     * encryption key
     *
     * @param probeType type of the probe
     * @param inputFields input fields values. Only field name and value do make sense. Other
     * @return target info object, if new target has been created, {@code null} if validation
     *     or rediscovery is requested instead of target addition.
     * @throws OperationFailedException if target addition failed due to another reason
     */
    @Nonnull
    @Override
    public TargetApiDTO migrateClassicTarget(@Nonnull String probeType,
                                             @Nonnull Collection<InputFieldApiDTO> inputFields)
        throws OperationFailedException, InterruptedException, IOException, GeneralSecurityException {
        List<InputFieldApiDTO> updatedInputFields = new ArrayList<>();
        List<InputFieldApiDTO> secretFields = new ArrayList<>();
        String encryptionKey = null;

        for (InputFieldApiDTO inputField : inputFields) {
            if (inputField.getIsSecret()) {
                secretFields.add(inputField);
                continue;
            }
            if (StringConstants.ENCRYPTION_KEY.equals(inputField.getName())) {
                encryptionKey =  inputField.getValue();
                continue;
            }
            updatedInputFields.add(inputField);
        }
        if (encryptionKey == null) {
            throw new OperationFailedException("Encryption key input field needs to be present");
        }
        for (InputFieldApiDTO secretField : secretFields) {
            String decryptedSecret = decryptClassicCiphertext(secretField.getValue(),
                encryptionKey);
            secretField.setValue(decryptedSecret);
            updatedInputFields.add(secretField);
        }
        return targetsService.createTarget(probeType, updatedInputFields);
    }

    @Nonnull
    private InputFieldApiDTO createDecryptedField(@Nonnull String decryptedPassword,
                                                  @Nonnull String fieldName) {
        final InputFieldApiDTO decryptedField = new InputFieldApiDTO();
        decryptedField.setName(fieldName);
        decryptedField.setValue(decryptedPassword);
        return decryptedField;
    }

    /**
     * Decrypts the given string using the encryption key. This method is used to decrypt cipher
     * texts that were encrypted in a classic instance
     * @param ciphertext text to decrypt
     * @param encryptionKey key to use to decrypt
     * @return The decrypted string
     * @throws IOException On file reading error
     * @throws GeneralSecurityException On any security-related error
     */
     static String decryptClassicCiphertext(final @Nonnull String ciphertext,
                                                  final @Nonnull String encryptionKey)
        throws IOException, GeneralSecurityException {
        final Cipher cipher = Cipher.getInstance(CLASSIC_CIPHER_ALGORITHM);
        final byte[] array = Base64.decode(ciphertext.getBytes(CHARSET_CRYPTO));
        final ByteBuffer buff = ByteBuffer.wrap(array);
        final int version = buff.getInt();

        int length = buff.getInt();
        if (length != PKCS5_SALT_LENGTH) {
            throw new SecurityException("Corrupted ciphertext.");
        }
        final byte[] salt = new byte[length];
        buff.get(salt);

        length = buff.getInt();
        if (length != cipher.getBlockSize()) {
            throw new SecurityException("Corrupted ciphertext.");
        }
        final byte[] nonce = new byte[length];
        buff.get(nonce);

        length = buff.getInt();
        final byte[] cipherBytes = new byte[length];
        buff.get(cipherBytes);
        KeyPair keyPair = getClassicEncryptionKeyForVMTurboInstance(encryptionKey);
        try {
            SecretKey key = generateSecretKey(keyPair.getEncryptionKey(), salt);
            final IvParameterSpec iv = new IvParameterSpec(nonce);
            cipher.init(Cipher.DECRYPT_MODE, key, iv);
            final byte[] plaintext = cipher.doFinal(cipherBytes);
            return new String(plaintext, CHARSET_CRYPTO);
        } catch (BadPaddingException e) {
            // In case we are using the new format, rethrow.
            if (!keyPair.hasEncryptionKeyForVMTurboInstanceJava7()) {
                throw new SecurityException(e);
            }
            SecretKey key = generateSecretKey(keyPair.getEncryptionKeyForVMTurboInstanceJava7(), salt);
            IvParameterSpec iv = new IvParameterSpec(nonce);
            cipher.init(Cipher.DECRYPT_MODE, key, iv);
            byte[] plaintext = cipher.doFinal(cipherBytes);
            return new String(plaintext, CHARSET_CRYPTO);
        }
    }

    private static KeyPair getClassicEncryptionKeyForVMTurboInstance(String encryptionKey) throws IOException {
        byte[] keyBytes = encryptionKey.getBytes();
        String keyStr = new String(keyBytes, CHARSET_CRYPTO);
        String encryptionKeyForVMTurboInstance = "";
        String encryptionKeyForVMTurboInstanceJava7 = null;
        // Check whether we have a sane key format.
        if (keyStr.startsWith(MAGIC_COOKIE_V1)) {
            encryptionKeyForVMTurboInstance = keyStr.substring(MAGIC_COOKIE_V1.length(),
                keyStr.length());
        } else {
            // Old style
            encryptionKeyForVMTurboInstance =
                new String(keyBytes, 0, CLASSIC_INSTANCE_KEY_LENGTH, CHARSET_CRYPTO);
            // Construct Java 7 version if needed.
            // Check for the Unicode substitution character: '\uFFFD'
            if (encryptionKeyForVMTurboInstance.endsWith("\uFFFD")) {
                int length = encryptionKeyForVMTurboInstance.length();
                for (; length > 0; length--) {
                    if (encryptionKeyForVMTurboInstance.charAt(length - 1) != '\uFFFD') {
                        break;
                    }
                }
                encryptionKeyForVMTurboInstanceJava7 =
                    encryptionKeyForVMTurboInstance.substring(0, length + 1);
            }
        }
        return new KeyPair(encryptionKeyForVMTurboInstance, encryptionKeyForVMTurboInstanceJava7);
    }

    private static SecretKey generateSecretKey(String password, byte[] salt) throws InvalidKeySpecException, NoSuchAlgorithmException {
        final KeySpec keySpec =
            new PBEKeySpec(password.toCharArray(), salt, PBKDF2_ITERATIONS, KEY_LENGTH);
        final SecretKeyFactory keyFactory =
            SecretKeyFactory.getInstance(CLASSIC_PBKDF2_DERIVATION_ALGORITHM);
        final byte[] keyBytes = keyFactory.generateSecret(keySpec).getEncoded();
        return  new SecretKeySpec(keyBytes, KEYSPEC_ALGORITHM);
    }

    /**
     * Object that contains both the encryption key and the corresponding encryption key for java 7.
     */
    private static class KeyPair {

         private final String encryptionKey;
         private final String encryptionKeyForVMTurboInstanceJava7;

         private KeyPair(String encryptionKey, String encryptionKeyForVMTurboInstanceJava7) {
             this.encryptionKey = encryptionKey;
             this.encryptionKeyForVMTurboInstanceJava7 = encryptionKeyForVMTurboInstanceJava7;
         }

        public String getEncryptionKey() {
            return encryptionKey;
        }

        public boolean hasEncryptionKeyForVMTurboInstanceJava7() {
            return encryptionKeyForVMTurboInstanceJava7 != null;
        }

        public String getEncryptionKeyForVMTurboInstanceJava7() {
            return encryptionKeyForVMTurboInstanceJava7;
        }
    }
}
