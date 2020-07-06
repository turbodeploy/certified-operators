package com.vmturbo.group.group;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import javax.annotation.Nonnull;

import org.apache.commons.codec.digest.DigestUtils;

import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;

/**
 * Utility class to perform hashing of discovered groups.
 */
public class DiscoveredGroupHash {

    private DiscoveredGroupHash() {}

    /**
     * Calculates a hash for the discovered group. We use group definition inside and sort
     * all the other fields (if they are collections)
     *
     * @param discoveredGroup group to hash
     * @return SHA256 hash value
     */
    @Nonnull
    public static byte[] hash(@Nonnull DiscoveredGroup discoveredGroup) {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(stream);
            dos.write(discoveredGroup.getDefinition().toByteArray());
            dos.write(0);
            dos.writeUTF(discoveredGroup.getSourceIdentifier());
            dos.write(0);
            for (long targetId : new TreeSet<>(discoveredGroup.getTargetIds())) {
                dos.writeLong(targetId);
            }
            dos.write(0);
            final List<MemberType> memberTypes = new ArrayList<>(
                    discoveredGroup.getExpectedMembers());
            memberTypes.sort(new MemberTypeComparator());
            for (MemberType memberType : memberTypes) {
                dos.write(memberType.toByteArray());
            }
            dos.write(0);
            dos.writeBoolean(discoveredGroup.isReverseLookupSupported());
            return DigestUtils.sha256(stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to operate with in-memory stream " + discoveredGroup,
                    e);
        }
    }

    /**
     * Comparator for {@link MemberType}. We do not care of the actual order. The only requirement
     * here is to make the order deterministic.
     */
    private static class MemberTypeComparator implements Comparator<MemberType> {
        @Override
        public int compare(MemberType o1, MemberType o2) {
            final int typeComparison = o1.getTypeCase().compareTo(o2.getTypeCase());
            if (typeComparison != 0) {
                return typeComparison;
            } else {
                if (o1.hasGroup()) {
                    return o1.getGroup().compareTo(o2.getGroup());
                } else if (o1.hasEntity()) {
                    return o1.getEntity() - o2.getEntity();
                } else {
                    return 0;
                }
            }
        }
    }
}
