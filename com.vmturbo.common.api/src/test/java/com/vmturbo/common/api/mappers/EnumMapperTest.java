package com.vmturbo.common.api.mappers;

import static org.junit.Assert.assertFalse;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTOREST.EntityState;

public class EnumMapperTest {


    private enum TestEnum {
        FOO,
        BAR
    }

    private final EnumMapper<TestEnum> enumMapper = EnumMapper.of(TestEnum.class);


    //Tests valueOf method using different acceptable options for description
    @Test
    public void valueOfShouldReturnTheEnumValueFromStrings() {
        Assert.assertEquals(TestEnum.FOO, enumMapper.valueOf("FOO").orElse(null));
        Assert.assertEquals(TestEnum.BAR, enumMapper.valueOf("BAR").orElse(null));
    }


     //Tests correct Enum const is returned using valueOf
    @Test
    public void valueOfShouldReturnTheEnumValueSimilarEnums() {
        Assert.assertEquals(TestEnum.FOO, enumMapper.valueOf(TestEnum.FOO).orElse(null));
        Assert.assertEquals(TestEnum.BAR, enumMapper.valueOf(TestEnum.BAR).orElse(null));
    }

    //Tests empty optional returned for unmatched descriptors
    @Test
    public void valueOfShouldReturnEmptyOptionalForUnmatchedDescriptor() {
        assertFalse(enumMapper.valueOf("foo bar").isPresent());
        assertFalse(enumMapper.valueOf(EntityState.FAILOVER).isPresent());
    }


    //Tests null is safely passed to valueOf
    @Test
    public void valueOfShouldBeNullSafe() {
        assertFalse(enumMapper.valueOf((String)null).isPresent());
        assertFalse(enumMapper.valueOf((TestEnum)null).isPresent());
    }
}