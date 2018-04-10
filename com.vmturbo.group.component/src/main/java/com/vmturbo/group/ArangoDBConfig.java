package com.vmturbo.group;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.arangodb.ArangoDB;
import com.arangodb.velocypack.VPackDeserializer;
import com.arangodb.velocypack.VPackSerializer;
import com.arangodb.velocypack.ValueType;
import com.arangodb.velocypack.exception.VPackBuilderException;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.group.arangodb.ArangoDBManager;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;

@Configuration
@Import({IdentityProviderConfig.class})
public class ArangoDBConfig {
    private static final String DOCUMENT_KEY_FIELD = "_key";
    private static final String POLICY_PROTO_FIELD ="policy_proto";
    private static final String GROUP_PROTO_FIELD ="group_proto";

    @Value("${arangodbPort:8529}")
    private int arangodbPort;

    @Value("${arangodbHost:arangodb}")
    private String arangodbHost;

    @Value("${arangodbUser:root}")
    private String arangodbUser;

    @Value("${authHost}")
    private String authHost;

    @Value("${authPort}")
    private int authPort;

    @Value("${authRetryDelaySecs}")
    private int authRetryDelaySecs;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public VPackSerializer<PolicyDTO.InputPolicy> inputPolicyVPackSerializer() {
        return (builder, attribute, policy, context) -> {
            builder.add(attribute, ValueType.OBJECT);
            builder.add(DOCUMENT_KEY_FIELD, Long.toString(policy.getId()));
            builder.add(POLICY_PROTO_FIELD, policy.toByteArray());

            if (policy.hasName()) {
                builder.add("name", policy.getName());
            }

            if (policy.hasTargetId()) {
                builder.add("targetId", policy.getTargetId());
            }

            // `policy.getPolicyDetailCase()` will always return a value.
            // Even if it is not set.
            builder.add("type", policy.getPolicyDetailCase().name());

            builder.close();
        };
    }

    @Bean
    public VPackDeserializer<PolicyDTO.InputPolicy> inputPolicyVPackDeserializer() {
        return (parent, vpack, context) -> {
            try {
                final byte[] bytes = vpack.get(POLICY_PROTO_FIELD).getAsBinary();
                return PolicyDTO.InputPolicy.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new VPackBuilderException(e);
            }
        };
    }

    @Bean
    public ArangoDriverFactory arangoDriverFactory() {
        final DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort,
            authRetryDelaySecs);
        final String arangodbPassword = dbPasswordUtil.getArangoDbRootPassword();

        // implements the functional interface ArangoDriverFactory::getDriver method
        // returns a new instance/connection every time the method is called
        // this is in order to try to prevent the connection to got stuck
        return () -> {
            return new ArangoDB.Builder()
                            .host(arangodbHost, arangodbPort)
                            .user(arangodbUser)
                            .password(arangodbPassword)
                            .registerSerializer(PolicyDTO.InputPolicy.class, inputPolicyVPackSerializer())
                            .registerDeserializer(PolicyDTO.InputPolicy.class, inputPolicyVPackDeserializer())
                            .registerSerializer(GroupDTO.Group.class, groupVPackSerializer())
                            .registerDeserializer(GroupDTO.Group.class, groupVPackDeserializer())
                            .build();
        };
    }

    @Bean
    public VPackSerializer<GroupDTO.Group> groupVPackSerializer() {
        return (builder, attribute, group, context) -> {
            builder.add(attribute, ValueType.OBJECT);
            builder.add(DOCUMENT_KEY_FIELD, Long.toString(group.getId()));
            builder.add(GROUP_PROTO_FIELD, group.toByteArray());

            builder.add("displayName", GroupProtoUtil.getGroupName(group));

            if (group.hasTargetId()) {
                builder.add("targetId", group.getTargetId());
            }

            builder.close();
        };
    }

    @Bean
    public VPackDeserializer<GroupDTO.Group> groupVPackDeserializer() {
        return (parent, vpack, context) -> {
            try {
                final byte[] bytes = vpack.get(GROUP_PROTO_FIELD).getAsBinary();
                return GroupDTO.Group.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new VPackBuilderException(e);
            }
        };
    }

    @Bean
    public PolicyStore policyStore() {
        return new PolicyStore(arangoDriverFactory(), groupDBDefinition(),
            identityProviderConfig.identityProvider());
    }

    @Bean
    public GroupStore groupStore() {
        return new GroupStore(arangoDriverFactory(), groupDBDefinition(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public ArangoDBManager arangoDBManager() {
        return new ArangoDBManager(arangoDriverFactory(), groupDBDefinition());
    }

    @Bean
    public GroupDBDefinition groupDBDefinition() {
        return com.vmturbo.group.ImmutableGroupDBDefinition.builder()
                .databaseName("group_policy")
                .policyCollection("policy")
                .groupCollection("group")
                .clusterCollection("cluster")
                .build();
    }
}
