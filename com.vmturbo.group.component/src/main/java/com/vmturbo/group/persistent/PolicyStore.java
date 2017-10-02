package com.vmturbo.group.persistent;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.group.ArangoDriverFactory;
import com.vmturbo.group.GroupDBDefinition;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.policy.DiscoveredPoliciesMapper;

public class PolicyStore {

    private final static Logger logger = LogManager.getLogger();

    private final static String ALL_POLICIES_QUERY = "FOR p IN @@policy_collection RETURN p";

    private final static String POLICIES_BY_TARGET_QUERY = "FOR p IN @@policy_collection "
                    + "FILTER p.targetId == @targetId "
                    + "RETURN p";

    private final ArangoDriverFactory arangoDriverFactory;
    private final GroupDBDefinition groupDBDefinition;
    private final IdentityProvider identityProvider;

    public PolicyStore(final ArangoDriverFactory arangoDriverFactory,
                       final GroupDBDefinition groupDBDefinitionArg,
                       final IdentityProvider identityProvider) {
        this.arangoDriverFactory = Objects.requireNonNull(arangoDriverFactory);
        this.groupDBDefinition = Objects.requireNonNull(groupDBDefinitionArg);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    /**
     * Update the set of policies discovered by a particular target.
     * The new set of policies will completely replace the old, even if the new set is empty.
     *
     * <p>See {@link TargetPolicyUpdate} for details on the update behavior.
     *
     * @param targetId The ID of the target that discovered the policies.
     * @param policyInfos The new set of {@link DiscoveredPolicyInfo}s.
     * @return a mapping of policy key (name) to policy OID
     * @throws DatabaseException If there is an error interacting with the database.
     */
    public void updateTargetPolicies(long targetId, List<DiscoveredPolicyInfo> policyInfos,
                                     Map<String, Long> groupOids)
            throws DatabaseException {
        logger.info("Updating policies discovered by {}. Got {} policies.",
            targetId, policyInfos.size());

        DiscoveredPoliciesMapper mapper = new DiscoveredPoliciesMapper(groupOids);
        List<InputPolicy> discoveredPolicies = policyInfos.stream()
                        .map(mapper::inputPolicy)
                        .filter(Optional::isPresent).map(Optional::get)
                        .collect(Collectors.toList());
        final TargetPolicyUpdate update = new TargetPolicyUpdate(targetId, identityProvider,
            discoveredPolicies, getDiscoveredByTarget(targetId));
        update.apply(this::store, this::delete);
        logger.info("Finished updating discovered groups.");
    }

    @Nonnull
    private Collection<InputPolicy> getDiscoveredByTarget(final long targetId) throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final Map<String, Object> bindVars = ImmutableMap.of(
                    "@policy_collection", groupDBDefinition.policyCollection(),
                    "targetId", targetId);

            final ArangoCursor<InputPolicy> cursor = arangoDB
                    .db(groupDBDefinition.databaseName())
                    .query(POLICIES_BY_TARGET_QUERY, bindVars, null, InputPolicy.class);

            return cursor.asListRemaining();
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to get policies discovered by target " + targetId, e);
        } finally {
            arangoDB.shutdown();
        }
    }

    public Optional<InputPolicy> get(final Long id) {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            return Optional.ofNullable(arangoDB.db(groupDBDefinition.databaseName())
                                               .collection(groupDBDefinition.policyCollection())
                                               .getDocument(Long.toString(id), PolicyDTO.InputPolicy.class));
        } catch (ArangoDBException e) {
            logger.error("Exception encountered while get policy with id " + id, e);
        } finally {
            arangoDB.shutdown();
        }

        return Optional.empty();
    }

    public Collection<PolicyDTO.InputPolicy> getAll() {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final Map<String, Object> bindVars = ImmutableMap.of(
                    "@policy_collection", groupDBDefinition.policyCollection());

            final ArangoCursor<PolicyDTO.InputPolicy> cursor = arangoDB
                    .db(groupDBDefinition.databaseName())
                    .query(ALL_POLICIES_QUERY, bindVars, null, PolicyDTO.InputPolicy.class);

            return cursor.asListRemaining();
        } catch (ArangoDBException e) {
            logger.error("Exception encountered while getting all the policies", e);
        } finally {
            arangoDB.shutdown();
        }

        return Collections.emptyList();
    }

    private void store(@Nonnull final InputPolicy policy) throws DatabaseException {
        save(policy.getId(), policy);
    }

    public boolean save(Long id, PolicyDTO.InputPolicy policy) {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final ArangoCollection policyCollection = arangoDB.db(groupDBDefinition.databaseName())
                                                          .collection(groupDBDefinition.policyCollection());
            final String policyID = Long.toString(id);

            if (policyCollection.documentExists(policyID)) {
                policyCollection.replaceDocument(policyID, policy);
            } else {
                policyCollection.insertDocument(policy);
            }

            return true;
        } catch (ArangoDBException e) {
            logger.error("Exception encountered while saving policy with id " + id, e);
        } finally {
            arangoDB.shutdown();
        }

        return false;
    }

    public boolean delete(Long id) {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            arangoDB.db(groupDBDefinition.databaseName())
                    .collection(groupDBDefinition.policyCollection())
                    .deleteDocument(Long.toString(id));
            return true;
        } catch (ArangoDBException e) {
            logger.error("Exception encountered while deleting policy with id " + id, e);
        } finally {
            arangoDB.shutdown();
        }

        return false;
    }

    public void deletePolicies(@Nonnull List<Long> ids) throws PolicyDeleteException {
        if (ids.isEmpty()) {
            return;
        }
        ArangoDB arangoDB = arangoDriverFactory.getDriver();
        // need to convert long list to string list in order to delete batch documents
        List<String> idStrList = ids.stream().map(String::valueOf).collect(Collectors.toList());
        try {
            arangoDB.db(groupDBDefinition.databaseName())
                .collection(groupDBDefinition.policyCollection())
                .deleteDocuments(idStrList, null, null);
        } catch (ArangoDBException e) {
            throw new PolicyDeleteException(ids);
        } finally {
            arangoDB.shutdown();
        }
    }

    /**
     * A custom exception for policy delete failure.
     */
    public static class PolicyDeleteException extends Exception {
        private PolicyDeleteException(List<Long> ids) {
            super("Failed to delete policies " + ids);
        }
    }
}
