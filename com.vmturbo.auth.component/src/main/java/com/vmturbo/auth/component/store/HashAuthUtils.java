package com.vmturbo.auth.component.store;

import javax.annotation.Nonnull;

import org.springframework.security.crypto.bcrypt.BCrypt;

/**
 * Static helper class to operate with password hashes.
 */
public class HashAuthUtils {
    /**
     * Use secure (and slow) algorithm for one-way password hashing.
     * TODO(Michael): Replace with an industry standard algorithm.
     *
     * @param plainText The plain text.
     * @return The secure hash.
     */
    public static @Nonnull
    String secureHash(@Nonnull String plainText) {
        String salt = BCrypt.gensalt(10);
        return BCrypt.hashpw(plainText, salt);
    }

    /**
     * Check whether the supplied password is the one that was hashed into hashed.
     *
     * @param hashed   The previously hashed password.
     * @param password The password in plain text.
     * @return {@code true} if the supplied password is the one that was hashed into hashed.
     */
    public static boolean checkSecureHash(@Nonnull String hashed, @Nonnull String password) {
        try {
            return BCrypt.checkpw(password, hashed);
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }


}
