package com.vmturbo.group.group;

import org.jooq.Converter;

import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Converter for group types.
 *
 * <p>WARNING!
 *
 * <p>We cannot trust the default Jooq's enum converter as it is relying on {@link Enum#ordinal()}.
 * If any value in the enum becomes deprecated, ordinal value of Enum changes!!!
 * Instead we are relying on protobuf's internal field numbers, which are preserved if any of
 * fields are removed from the DTO object.
 */
public class GroupTypeConverter implements Converter<Integer, GroupType> {

    @Override
    public GroupType from(Integer databaseObject) {
        return GroupType.forNumber(databaseObject);
    }

    @Override
    public Integer to(GroupType userObject) {
        return userObject.getNumber();
    }

    @Override
    public Class<Integer> fromType() {
        return Integer.class;
    }

    @Override
    public Class<GroupType> toType() {
        return GroupType.class;
    }
}
