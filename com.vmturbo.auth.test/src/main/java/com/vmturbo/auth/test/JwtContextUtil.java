package com.vmturbo.auth.test;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.ImmutableList;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;


/**
 * Initialize the JWT Security Context for a unit test.
 * Given an RPC Service and a user OID to represent the "current user oid",
 * <ol>
 *     <li>create a JWT token</li>
 *     <li>add to the Spring {@link SecurityContextHolder}</li>
 *     <li>create the private / public keys required</li>
 *     <li>publish the public key to a mock IApiAuthStore</li>
 *     <li>create an instance of the JwtServerInterceptor</li>
 *     <li>initialized with the mock IApiAuthStore</li>
 *     <li>create a GRPC Server Instance running the given</li>
 *     <li>RPC Service, and create a 'channel' for the service</li>
 * </ol>
 * In the @Before method, call
 * <pre>
 *       private WidgetsetRpcService widgetsetRpcService = new WidgetsetRpcService(widgetsetStore);
 *       private JwtContextUtil jwtContextUtil;
 *      {@literal @}Before
 *       public void setup() {
 *             jwtContextUtil = new JwtContextUtil();
 *             jwtContextUtil.setupSecurityContext(widgetsetRpcService, userOid);
 *             widgetsetRpcClient = WidgetsetsServiceGrpc.newBlockingStub(jwtContextUtil.getChannel())
 *                                                       .withInterceptors(new JwtClientInterceptor());
 *       }
 * </pre>
 * and in the @After method, call
 * <pre>
 *      {@literal @}After
 *       public void shutdown() {
 *             jwtContextUtil.shutdown();
 *       }
 * </pre>

 **/
public class JwtContextUtil {

    public static final String ADMIN = "admin";
    public static final String ADMINISTRATOR = "ADMINISTRATOR";
    public static final String IP_ADDRESS = "10.10.10.1";

    private JWTAuthorizationToken token;
    private String pubKeyStr;

    private Server grpcServer;

    private ManagedChannel channel;

    private final Logger logger = LogManager.getLogger();

    public <S extends BindableService> void setupSecurityContext(@Nonnull S rpcService,
                                                                 long userOid,
                                                                 @Nonnull String userid) throws Exception {

        String userOidString = Long.toString(userOid);

        // setup public/private key pair
        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);

        // build JWT token
        String compact = Jwts.builder()
                .setSubject(userid)
                .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of(ADMINISTRATOR))
                .claim(JWTAuthorizationVerifier.UUID_CLAIM, userOidString)
                .claim(JWTAuthorizationVerifier.IP_ADDRESS_CLAIM, IP_ADDRESS) // add IP address
                .setExpiration(getTestKeyExpiration())
                .signWith(SignatureAlgorithm.ES256, keyPair.getPrivate())
                .compressWith(CompressionCodecs.GZIP)
                .compact();

        // Encode the public key.
        pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());

        // store JWT token for gRPC client
        token = new JWTAuthorizationToken(compact);

        // Setup caller authentication
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        grantedAuths.add(new SimpleGrantedAuthority("ROLE_NONADMINISTRATOR"));
        AuthUserDTO user = new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "admin", null, null, userOidString,
                compact, new ArrayList<>(), null);
        Authentication authentication = new UsernamePasswordAuthenticationToken(user, "***", grantedAuths);

        // populate security context, so the client interceptor can get the JWT token.
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // mock providing public key to auth store
        final IAuthStore apiAuthStore = new TestAuthStore(getPubKeyStr());

        // setup JWT ServerInterceptor
        final JwtServerInterceptor jwtInterceptor = new JwtServerInterceptor(apiAuthStore);

        // start secure gRPC
        final String name = "grpc-security-JWT-test";
        final InProcessServerBuilder serverBuilder = InProcessServerBuilder.forName(name);
        serverBuilder.addService(ServerInterceptors.intercept(rpcService, jwtInterceptor));
        grpcServer = serverBuilder.build();
        grpcServer.start();

        channel = InProcessChannelBuilder.forName(name).build();
    }

    /**
     * Calculate an expiration date one day in the future for the key pair to expire.
     *
     * @return a date one day in the future from now.
     */
    private static Date getTestKeyExpiration() {
        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();
        return dt;
    }

    public JWTAuthorizationToken getToken() {
        return token;
    }

    public String getPubKeyStr() {
        return pubKeyStr;
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public void shutdown() {
        logger.info("shutting down gRPC channel and server");
        channel.shutdown();
        grpcServer.shutdown();

    }

    private static class TestAuthStore implements IAuthStore {

        private final String pubKeyStr;

        public TestAuthStore(String pubKeyStr) {
            this.pubKeyStr = pubKeyStr;
        }

        @Override
        public String retrievePublicKey() {
            return pubKeyStr;
        }

        @Override
        public Optional<String> retrievePublicKey(final String namespace) {
            return Optional.empty();
        }
    }
}
