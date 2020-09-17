package com.vmturbo.mediation.actionscript;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.client.keyverifier.ServerKeyVerifier;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.config.keys.KeyEntryResolver;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.config.keys.PublicKeyEntryDecoder;
import org.bouncycastle.util.encoders.Base64;

// TODO test with all key algorithms supported by OpenSSH ssh-keygen

/**
 * Class to manage host key validation in SSH sessions.
 *
 * <p>The user has the option of including a host key string (as it would appear in known-hosts file) when
 * defining an action script target. If that is done, all SSH sessions will use that key for host
 * authentication. Otherwise, any host key will be accepted during the first SSH session established
 * after probe has been instantiated, and that key will be used to fill in the optional target field.
 * That same key will be required thenceforth.</p>
 *
 * <p>If the host key ever changes (as is recommended), the target defintion must be updated so the new
 * key will be accepted. That can be done by editing the target and either filling in the new host key
 * string or removing the existing value.</p>
 */
public class AcceptAnyOrGivenServerKeyVerifier implements ServerKeyVerifier {
    private static Logger logger = LogManager.getLogger(AcceptAnyOrGivenServerKeyVerifier.class);

    // Finishing the host key verification feature was pushed to next sprint, so rather than
    // discard these chagnes, we'll just disable the capailities and accept all server keys for
    // now.
    // TODO Remove this when the feature is implemented
    private final boolean HOST_KEYS_IMPLEMENTED = false;

    // host to which a session is being established. This will always be the same host (unless the
    // target is edited), so we don't really keep track of it, except to use it in log messages
    private final String host;
    // public key used by us during host authentication when creating an SSH session. Null if no key
    // has yet been fixed, or a provided key string could not be parsed to produce a valid key
    private PublicKey requiredKey;
    // true if a key string was supplied in the target definition, but we could not parse it
    private boolean badKey = false;

    /**
     * Create an instance of this verifier
     *
     * @param requiredKeyString the string form of the required host key, or null to accept the first
     *                          key we encounter and then require it going forward
     * @param host              host name/ip we will be connecting to
     */
    public AcceptAnyOrGivenServerKeyVerifier(@Nullable final String requiredKeyString, String host) {
        if (!HOST_KEYS_IMPLEMENTED) {
            this.requiredKey = requiredKeyString != null ? parsePublicKey(requiredKeyString) : null;
            if (requiredKeyString != null && requiredKey == null) {
                // if we failed to create a public key, we should fail host key validation until the
                // situation is corrected. A null value for requiredKey normally means accept any
                // host key and then require that same key from that point forward. This flag takes
                // care of that situation
                logger.warn(String.format("Failed to parse supplied host key for SSH authentication; action script target cannot be used until this is addrressed. Unparsable key string: %s", requiredKeyString));
                this.badKey = true;
            }
        }
        this.host = host;
    }

    /**
     * Decide whether to accept a key provided by an SSH host to which we are attempting a connection
     *
     * @param sshClientSession session performing authentication
     * @param remoteAddress    address of SSH server
     * @param serverKey        key provided by SSH server for host authentication
     * @return true if the presented key is acceptable
     */
    @Override
    public boolean verifyServerKey(final ClientSession sshClientSession, final SocketAddress remoteAddress, final PublicKey serverKey) {
        if (!HOST_KEYS_IMPLEMENTED) {
            return true;
        }
        if (badKey) {
            logger.warn(String.format("Rejected host key for host %s because required host key could not be resolved", host));
            return false;
        }
        if (requiredKey == null) {
            this.requiredKey = serverKey;
            logger.warn(String.format("Accepted host key for host %s; will require it from now on for this target. Key string: %s", host, requiredKey.toString()));
            // TODO figure out how to get this key into the probe account values saved for this target
            return true;
        } else if (requiredKey.equals(serverKey)) {
            logger.info(String.format("Accepted expected host key for host %s: %s", host, createKeyString(serverKey)));
            return true;
        } else {
            logger.warn(String.format("Rejected host key for host %s; expected %s; got %s", host, requiredKey, createKeyString(serverKey)));
            return false;
        }
    }


    public String getAcceptedKeyString() {
        return requiredKey != null ? createKeyString(requiredKey) : null;
    }

    /**
     * Attempt to parse the given key string (as would be found in an ssh known-hosts file) to yield a public key
     *
     * <p>The string should include at least two space-separated segments. The first is a plain-text name
     * for the key algorithm/format, while the second is a base-64 encoding of the binary key value, packaged
     * in an algorithm-specific fashion. Additional parts (which may be used for tagging, etc.) are ignored.</p>
     *
     * @param publicKeyString the key string to be parsed
     * @return parsed public key, or null if parsing fails
     */
    private PublicKey parsePublicKey(String publicKeyString) {
        String[] parts = publicKeyString.split("\\s+");
        byte[] keyValue = null;
        // TODO attempt to parse first part if only one part
        if (parts.length >= 2) {
            keyValue = Base64.decode(parts[1].getBytes());
        }
        try (InputStream bais = new ByteArrayInputStream(keyValue)) {
            String keyType = KeyEntryResolver.decodeString(bais);
            PublicKeyEntryDecoder<?, ?> decoder = KeyUtils.getPublicKeyEntryDecoder(keyType);
            if (decoder == null) {
                throw new NoSuchAlgorithmException("Unsupported key type (" + keyType + ")");
            }
            return decoder.decodePublicKey(keyType, bais);
        } catch (Exception e) {
            logger.warn("Failed to resolve supplied SSH host public key", e);
            return null;
        }
    }

    private String createKeyString(PublicKey key) {
        // TODO implement
        return null;
    }

}
