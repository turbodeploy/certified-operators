package com.vmturbo.protoc.pojo.gen.testjavaopt2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassDTO.Bar.MyOneOfCase;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassDTO.Bar.Plant;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJOPkg.Bar;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJOPkg.Bar.BlahPOJO;
import com.vmturbo.protoc.pojo.gen.testjavaopt2.BarClassPOJOPkg.Bar.OtherMessage;

/**
 * BarClassPOJOTest.
 */
public class BarClassPOJOTest {

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final Bar bar = new Bar();
    private final BarClassDTO.Bar defaultProto = BarClassDTO.Bar.getDefaultInstance();

    private final ByteString fooBytes = ByteString.copyFromUtf8("foo");
    private final ByteString barBytes = ByteString.copyFromUtf8("bar");

    /**
     * testHasPrimitive.
     */
    @Test
    public void testHasPrimitive() {
        assertFalse(bar.hasBar());
        bar.setBar(0L);
        assertTrue(bar.hasBar());

        bar.setBar(1L);
        assertTrue(bar.hasBar());
    }

    /**
     * testHasEnum.
     */
    @Test
    public void testHasEnum() {
        assertFalse(bar.hasGardenPlant());
        bar.setGardenPlant(Plant.BUSH);
        assertTrue(bar.hasGardenPlant());
        assertFalse(bar.hasBar());
    }

    /**
     * testHasString.
     */
    @Test
    public void testHasString() {
        assertFalse(bar.hasSomeString());
        bar.setSomeString("");
        assertTrue(bar.hasSomeString());
    }

    /**
     * testHasBoolean.
     */
    @Test
    public void testHasBoolean() {
        assertFalse(bar.hasMyBoolean());
        bar.setMyBoolean(bar.getMyBoolean());
        assertTrue(bar.hasMyBoolean());
    }

    /**
     * testHasSubMessage.
     */
    @Test
    public void testHasSubMessage() {
        assertFalse(bar.hasMyOtherMessage());
        bar.setMyOtherMessage(new OtherMessage());
        assertTrue(bar.hasMyOtherMessage());
    }

    /**
     * testClearPrimitiveField.
     */
    @Test
    public void testClearPrimitiveField() {
        bar.setBar(0L);
        assertTrue(bar.hasBar());
        bar.clearBar();
        assertFalse(bar.hasBar());
    }

    /**
     * testClearEnum.
     */
    @Test
    public void testClearEnum() {
        bar.setGardenPlant(Plant.CHRISTMAS_CACTUS);
        assertTrue(bar.hasGardenPlant());
        bar.clearGardenPlant();
        assertFalse(bar.hasGardenPlant());
    }

    /**
     * testClearString.
     */
    @Test
    public void testClearString() {
        bar.setSomeString("foo");
        assertTrue(bar.hasSomeString());
        bar.clearSomeString();
        assertFalse(bar.hasSomeString());
    }

    /**
     * testClearBoolean.
     */
    @Test
    public void testClearBoolean() {
        bar.setMyBoolean(false);
        assertTrue(bar.hasMyBoolean());
        bar.clearMyBoolean();
        assertFalse(bar.hasMyBoolean());
    }

    /**
     * testClearSubMessage.
     */
    @Test
    public void testClearSubMessage() {
        bar.setMyOtherMessage(new OtherMessage());
        assertTrue(bar.hasMyOtherMessage());
        bar.clearMyOtherMessage();
        assertFalse(bar.hasMyOtherMessage());
    }

    /**
     * testGetSetPrimitiveFieldWithDefault.
     */
    @Test
    public void testGetSetPrimitiveFieldWithDefault() {
        assertEquals(defaultProto.getMyDoubleWithDefault(),
            bar.getMyDoubleWithDefault(), 0);
        bar.setMyDoubleWithDefault(0.5);
        assertEquals(0.5, bar.getMyDoubleWithDefault(), 0);
        bar.clearMyDoubleWithDefault();
        assertEquals(defaultProto.getMyDoubleWithDefault(),
            bar.getMyDoubleWithDefault(), 0);
    }

    /**
     * testGetSetBytesFieldWithDefault.
     */
    @Test
    public void testGetSetBytesFieldWithDefault() {
        assertEquals(defaultProto.getBytesWithDefault(),
            bar.getBytesWithDefault());
        bar.setBytesWithDefault(fooBytes);
        assertEquals(fooBytes, bar.getBytesWithDefault());
        bar.clearBytesWithDefault();
        assertEquals(defaultProto.getBytesWithDefault(),
            bar.getBytesWithDefault());
    }

    /**
     * testOneOfMessageVariant.
     */
    @Test
    public void testOneOfMessageVariant() {
        assertEquals(MyOneOfCase.MYONEOF_NOT_SET, bar.getMyOneOfCase());
        assertEquals(defaultProto.getBoolVariant(), bar.getBoolVariant());
        assertEquals(defaultProto.getStringVariant(), bar.getStringVariant());
        assertEquals(defaultProto.getIntVariant(), bar.getIntVariant());
        assertEquals(defaultProto.getNoDefaultString(), bar.getNoDefaultString());
        assertEquals(defaultProto.getOtherMessage(), bar.getOtherMessage().toProto());
        assertEquals(defaultProto.getBlah(), bar.getBlah().toProto());
        assertEquals(defaultProto.getBytesVariant(), bar.getBytesVariant());
        assertEquals(defaultProto.getBytesVariantWithDefualt(), bar.getBytesVariantWithDefualt());

        bar.setStringVariant("foo");
        assertEquals(MyOneOfCase.STRING_VARIANT, bar.getMyOneOfCase());
        bar.setIntVariant(2);
        assertEquals(MyOneOfCase.INT_VARIANT, bar.getMyOneOfCase());
        bar.setBoolVariant(false);
        assertEquals(MyOneOfCase.BOOL_VARIANT, bar.getMyOneOfCase());

        // Clearing string and int variants when the bool variant is set does nothing
        bar.clearStringVariant();
        bar.clearIntVariant();
        assertEquals(MyOneOfCase.BOOL_VARIANT, bar.getMyOneOfCase());

        bar.clearBoolVariant();
        assertEquals(MyOneOfCase.MYONEOF_NOT_SET, bar.getMyOneOfCase());

        bar.setOtherMessage(new OtherMessage().setMyInt(1));
        assertEquals(1, bar.getOtherMessage().getMyInt());
        assertEquals(MyOneOfCase.OTHER_MESSAGE, bar.getMyOneOfCase());

        bar.setBlah(new BlahPOJO().setStr("blah"));
        assertEquals("blah", bar.getBlah().getStr());
        assertEquals(MyOneOfCase.BLAH, bar.getMyOneOfCase());

        bar.setBytesVariant(barBytes);
        assertEquals(barBytes, bar.getBytesVariant());
        assertEquals(MyOneOfCase.BYTES_VARIANT, bar.getMyOneOfCase());
    }

    /**
     * testOneOfIntVariant.
     */
    @Test
    public void testOneOfIntVariant() {
        assertFalse(bar.hasIntVariant());
        assertEquals(0, bar.getIntVariant());
        bar.setIntVariant(3);
        assertTrue(bar.hasIntVariant());
        assertEquals(3, bar.getIntVariant());
    }

    /**
     * testOneOfBoolVariant.
     */
    @Test
    public void testOneOfBoolVariant() {
        assertFalse(bar.hasBoolVariant());
        assertEquals(defaultProto.getBoolVariant(), bar.getBoolVariant());
        bar.setBoolVariant(false);
        assertTrue(bar.hasBoolVariant());
        assertFalse(bar.getBoolVariant());
    }

    /**
     * testOneOfStringVariant.
     */
    @Test
    public void testOneOfStringVariant() {
        assertFalse(bar.hasStringVariant());
        assertFalse(bar.hasNoDefaultString());

        assertEquals(defaultProto.getStringVariant(), bar.getStringVariant());
        assertEquals(defaultProto.getNoDefaultString(), bar.getNoDefaultString());

        bar.setStringVariant("bar");
        assertTrue(bar.hasStringVariant());
        assertEquals("bar", bar.getStringVariant());
    }

    /**
     * testOneOfEnumVariant.
     */
    @Test
    public void testOneOfEnumVariant() {
        assertFalse(bar.hasYourPlant());
        assertFalse(bar.hasThirdPlant());

        assertEquals(defaultProto.getYourPlant(), bar.getYourPlant());
        assertEquals(defaultProto.getThirdPlant(), bar.getThirdPlant());

        bar.setYourPlant(Plant.OAK);
        assertTrue(bar.hasYourPlant());
        assertFalse(bar.hasThirdPlant());

        assertEquals(Plant.OAK, bar.getYourPlant());
        assertEquals(defaultProto.getThirdPlant(), bar.getThirdPlant());

        bar.setThirdPlant(Plant.OAK);
        assertFalse(bar.hasYourPlant());
        assertTrue(bar.hasThirdPlant());

        assertEquals(Plant.OAK, bar.getThirdPlant());
        assertEquals(defaultProto.getYourPlant(), bar.getYourPlant());
    }

    /**
     * We should not be able to modify the lists returned by the list getter when it is empty.
     */
    @Test
    public void testModifyEmptyRepeatedList() {
        expectedException.expect(UnsupportedOperationException.class);
        bar.getRepeatedIntList()
            .add(1);
    }

    /**
     * We should not be able to modify the lists returned by the list getter when it is not empty.
     */
    @Test
    public void testModifyNonEmptyRepeatedList() {
        expectedException.expect(UnsupportedOperationException.class);
        bar.addRepeatedInt(1)
            .getRepeatedIntList()
            .add(1);
    }

    /**
     * We should not be able to add a null element to a list for a repeated field.
     */
    @Test
    public void testAddNull() {
        expectedException.expect(NullPointerException.class);
        bar.addRepeatedBlah(null);
    }

    /**
     * We should not be able to add a null element to a list for a repeated field
     * even when adding a collection.
     */
    @Test
    public void testAddAllNull() {
        expectedException.expect(NullPointerException.class);
        bar.addAllRepeatedInt(Arrays.asList(1, 2, 3, null, 5));
    }

    /**
     * Test that we get the correct exception when trying to set a value at an index out of bounds.
     */
    @Test
    public void testSetAtIndexOutOfBoundsOnEmpty() {
        expectedException.expect(IndexOutOfBoundsException.class);
        expectedException.expectMessage("Index: 3, Size: 0");
        bar.setRepeatedInt(3, 5);
    }

    /**
     * Test that we get the correct exception when trying to set a value at an index out of bounds.
     */
    @Test
    public void testSetAtIndexOutOfBoundsOnNonEmpty() {
        expectedException.expect(IndexOutOfBoundsException.class);
        expectedException.expectMessage("Index: 3, Size: 2");
        bar.addRepeatedInt(1);
        bar.addRepeatedInt(2);
        bar.setRepeatedInt(3, 5);
    }

    /**
     * testRepeatedInt.
     */
    @Test
    public void testRepeatedInt() {
        assertEquals(0, bar.getRepeatedIntList().size());
        assertEquals(1234, bar.addRepeatedInt(1234).getRepeatedInt(0).intValue());
        assertEquals(1234, bar.getRepeatedIntList().get(0).intValue());
        assertEquals(1, bar.getRepeatedIntCount());
        assertEquals(12345, bar.setRepeatedInt(0, 12345).getRepeatedInt(0).intValue());
        assertEquals(0, bar.clearRepeatedInt().getRepeatedIntCount());
    }

    /**
     * testRepeatedBool.
     */
    @Test
    public void testRepeatedBool() {
        assertEquals(0, bar.getRepeatedBoolList().size());
        assertTrue(bar.addRepeatedBool(true).getRepeatedBool(0));
        assertTrue(bar.getRepeatedBoolList().get(0));
        assertEquals(1, bar.getRepeatedBoolCount());
        assertFalse(bar.setRepeatedBool(0, false).getRepeatedBool(0));
        assertEquals(0, bar.clearRepeatedBool().getRepeatedBoolCount());
    }

    /**
     * testRepeatedString.
     */
    @Test
    public void testRepeatedString() {
        assertEquals(0, bar.getRepeatedStringList().size());
        assertEquals("1234", bar.addRepeatedString("1234").getRepeatedString(0));
        assertEquals("1234", bar.getRepeatedStringList().get(0));
        assertEquals(1, bar.getRepeatedStringCount());
        assertEquals("foo", bar.setRepeatedString(0, "foo").getRepeatedString(0));
        assertEquals(0, bar.clearRepeatedString().getRepeatedStringCount());
    }

    /**
     * testRepeatedMessage.
     */
    @Test
    public void testRepeatedMessage() {
        assertEquals(0, bar.getRepeatedBlahList().size());
        assertEquals(new BlahPOJO().setStr("foo"),
            bar.addRepeatedBlah(new BlahPOJO().setStr("foo")).getRepeatedBlah(0));
        assertEquals(new BlahPOJO().setStr("foo"), bar.getRepeatedBlahList().get(0));
        assertEquals(1, bar.getRepeatedBlahCount());
        assertEquals(new BlahPOJO().setStr("bar"),
            bar.setRepeatedBlah(0, new BlahPOJO().setStr("bar")).getRepeatedBlah(0));
        assertEquals(0, bar.clearRepeatedBlah().getRepeatedBlahCount());
    }

    /**
     * testIntEquals.
     */
    @Test
    public void testIntEquals() {
        final Bar other = new Bar();

        bar.setBar(1234L);
        bar.setIntVariant(3);
        assertNotEquals(other, bar);

        other.setBar(1234L);
        other.setIntVariant(3);
        assertEquals(other, bar);
    }

    /**
     * testBooleanEquals.
     */
    @Test
    public void testBooleanEquals() {
        final Bar other = new Bar();

        bar.setMyBoolean(true);
        bar.setBoolVariant(false);
        assertNotEquals(other, bar);

        other.setMyBoolean(true);
        other.setBoolVariant(false);
        assertEquals(other, bar);
    }

    /**
     * testStringEquals.
     */
    @Test
    public void testStringEquals() {
        final Bar other = new Bar();

        bar.setSomeString("foo");
        bar.setStringVariant("bar");
        assertNotEquals(other, bar);

        other.setSomeString("foo");
        other.setStringVariant("bar");
        assertEquals(other, bar);
    }

    /**
     * testByteStringEquals.
     */
    @Test
    public void testByteStringEquals() {
        final Bar other = new Bar();

        bar.setMyBytes(fooBytes);
        bar.setBytesWithDefault(barBytes);
        assertNotEquals(other, bar);

        other.setMyBytes(fooBytes);
        other.setBytesWithDefault(barBytes);
        assertEquals(other, bar);
    }

    /**
     * testEnumEquals.
     */
    @Test
    public void testEnumEquals() {
        final Bar other = new Bar();

        bar.setGardenPlant(Plant.POTATO);
        bar.setYourPlant(Plant.BUSH);
        assertNotEquals(other, bar);

        other.setGardenPlant(Plant.POTATO);
        other.setYourPlant(Plant.BUSH);
        assertEquals(other, bar);
    }

    /**
     * testMessageEquals.
     */
    @Test
    public void testMessageEquals() {
        final Bar other = new Bar();

        bar.setMyOtherMessage(new OtherMessage().setMyInt(2));
        bar.setBlah(new BlahPOJO().setStr("foo"));
        assertNotEquals(other, bar);

        other.setMyOtherMessage(new OtherMessage().setMyInt(2));
        other.setBlah(new BlahPOJO().setStr("foo"));
        assertEquals(other, bar);
    }

    /**
     * testMapContainsOnEmpty.
     */
    @Test
    public void testMapContainsOnEmpty() {
        assertFalse(bar.containsMyMap("foo"));
    }

    /**
     * testNullKeyMapContains.
     */
    @Test
    public void testNullKeyMapContains() {
        expectedException.expect(NullPointerException.class);
        bar.containsMyMap(null);
    }

    /**
     * testContains.
     */
    @Test
    public void testContains() {
        assertFalse(bar.containsMyMap("foo"));
        bar.putMyMap("foo", new OtherMessage());
        assertTrue(bar.containsMyMap("foo"));
    }

    /**
     * testPutNullKey.
     */
    @Test
    public void testPutNullKey() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Key cannot be null");
        bar.putMyMap(null, new OtherMessage());
    }

    /**
     * testPutNullValue.
     */
    @Test
    public void testPutNullValue() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("Value cannot be null");
        bar.putMyMap("foo", null);
    }

    /**
     * testPutAllNull.
     */
    @Test
    public void testPutAllNull() {
        expectedException.expect(NullPointerException.class);
        bar.putAllMyMap(null);
    }

    /**
     * testPutAllWithNullValue.
     */
    @Test
    public void testPutAllWithNullValue() {
        expectedException.expect(NullPointerException.class);
        final Map<String, OtherMessage> map = new HashMap<>();
        map.put("foo", null);

        bar.putAllMyMap(map);
    }

    /**
     * testPutAllWithNullKey.
     */
    @Test
    public void testPutAllWithNullKey() {
        expectedException.expect(NullPointerException.class);
        final Map<String, OtherMessage> map = new HashMap<>();
        map.put(null, new OtherMessage().setMyInt(1));

        bar.putAllMyMap(map);
    }

    /**
     * testPutAll.
     */
    @Test
    public void testPutAll() {
        assertEquals(0, bar.getMyMapCount());
        final Map<String, OtherMessage> map1 = ImmutableMap.of(
            "foo", new OtherMessage().setMyInt(1),
            "bar", new OtherMessage().setMyInt(2)
        );

        bar.putAllMyMap(map1);
        assertTrue(bar.containsMyMap("foo"));
        assertTrue(bar.containsMyMap("bar"));
        assertEquals(2, bar.getMyMapCount());

        final Map<String, OtherMessage> map2 = ImmutableMap.of(
            "foo", new OtherMessage().setMyInt(3),
            "baz", new OtherMessage().setMyInt(4),
            "quux", new OtherMessage().setMyInt(5)
        );

        bar.putAllMyMap(map2);
        assertTrue(bar.containsMyMap("foo"));
        assertTrue(bar.containsMyMap("bar"));
        assertTrue(bar.containsMyMap("baz"));
        assertTrue(bar.containsMyMap("quux"));
        assertEquals(new OtherMessage().setMyInt(3), bar.getMyMapOrThrow("foo"));
        assertEquals(4, bar.getMyMapCount());
    }

    /**
     * testClearMap.
     */
    @Test
    public void testClearMap() {
        bar.clearPrimitivesMap();
        assertEquals(0, bar.getPrimitivesMapCount());

        bar.putPrimitivesMap(1, true);
        bar.putPrimitivesMap(2, false);
        assertEquals(2, bar.getPrimitivesMapCount());

        bar.clearPrimitivesMap();
        assertEquals(0, bar.getPrimitivesMapCount());
    }

    /**
     * testCannotModifyEmptyMapDirectly.
     */
    @Test
    public void testCannotModifyEmptyMapDirectly() {
        expectedException.expect(UnsupportedOperationException.class);
        bar.getMyMapMap().put("foo", new OtherMessage().setMyInt(1));
    }

    /**
     * testCannotModifyNonEmptyMapDirectly.
     */
    @Test
    public void testCannotModifyNonEmptyMapDirectly() {
        bar.putMyMap("foo", new OtherMessage().setMyInt(1));

        expectedException.expect(UnsupportedOperationException.class);
        bar.getMyMapMap().clear();
    }

    /**
     * testMapEquals.
     */
    @Test
    public void testMapEquals() {
        final Bar other = new Bar();
        assertEquals(bar, other);

        bar.putMyMap("foo", new OtherMessage().setMyInt(1));
        assertNotEquals(bar, other);

        other.putMyMap("foo", new OtherMessage().setMyInt(1));
        assertEquals(bar, other);

        bar.putPrimitivesMap(1, false);
        bar.putPrimitivesMap(2, true);
        assertNotEquals(bar, other);

        other.putPrimitivesMap(1, false);
        other.putPrimitivesMap(2, true);
        assertEquals(bar, other);
    }

    /**
     * testRepeatedEquals.
     */
    @Test
    public void testRepeatedEquals() {
        final Bar other = new Bar();

        bar.addAllRepeatedInt(Arrays.asList(1, 2, 3, 4));
        assertNotEquals(bar, other);
        other.addAllRepeatedInt(Arrays.asList(1, 2, 3, 4));
        assertEquals(bar, other);

        bar.addRepeatedBlah(new BlahPOJO().setStr("foo"));
        assertNotEquals(bar, other);
        other.addRepeatedBlah(new BlahPOJO().setStr("foo"));
        assertEquals(bar, other);

        bar.addAllRepeatedBool(Arrays.asList(true, false, true));
        assertNotEquals(bar, other);
        other.addAllRepeatedBool(Arrays.asList(true, false, true));
        assertEquals(bar, other);

        bar.addAllRepeatedString(Arrays.asList("foo", "bar", "baz"));
        assertNotEquals(bar, other);
        other.addAllRepeatedString(Arrays.asList("foo", "bar", "baz"));
        assertEquals(bar, other);
    }

    /**
     * testIntDefault.
     */
    @Test
    public void testIntDefault() {
        Bar.getDefaultInstance().setBar(1234L);
        assertNotEquals(1234L, Bar.getDefaultInstance().getBar());
    }

    /**
     * testToFromProtoRepeatedAndMapFields.
     */
    @Test
    public void testToFromProtoRepeatedAndMapFields() {
        final BarClassDTO.Bar proto = defaultProto.toBuilder()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(BarClassDTO.Bar.BlahDTO.newBuilder().setStr("foo").build())
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .putMyMap("foo", new OtherMessage().setMyInt(1).toProto())
            .putMyMap("bar", new OtherMessage().setMyInt(2).toProto())
            .putPrimitivesMap(3, false)
            .putPrimitivesMap(4, true)
            .build();

        final Bar bar = Bar.fromProto(proto);
        assertEquals(Arrays.asList(1, 2, 3, 4), bar.getRepeatedIntList());
        assertEquals(new BlahPOJO().setStr("foo"), bar.getRepeatedBlah(0));
        assertEquals(Arrays.asList(true, false, true), bar.getRepeatedBoolList());
        assertEquals(Arrays.asList("foo", "bar", "baz"), bar.getRepeatedStringList());
        assertEquals(ImmutableMap.of("foo", new OtherMessage().setMyInt(1),
            "bar", new OtherMessage().setMyInt(2)), bar.getMyMapMap());
        assertEquals(ImmutableMap.of(3, false, 4, true), bar.getPrimitivesMapMap());
        assertEquals(proto, bar.toProto());
    }

    /**
     * testToByteArray.
     */
    @Test
    public void testToByteArray() {
        final BarClassDTO.Bar proto = defaultProto.toBuilder()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(BarClassDTO.Bar.BlahDTO.newBuilder().setStr("foo").build())
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .build();

        final Bar bar = new Bar()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahPOJO().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes));

        final byte[] protoBytes = proto.toByteArray();
        final byte[] pojoBytes = bar.toByteArray();
        assertEquals(protoBytes.length, pojoBytes.length);
        for (int i = 0; i < protoBytes.length; i++) {
            assertEquals(protoBytes[i], pojoBytes[i]);
        }
    }

    /**
     * testHashing.
     */
    @Test
    public void testHashing() {
        final Bar bar = new Bar()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahPOJO().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .setDoubleVariant(3.0)
            .putMyMap("foo", new OtherMessage().setMyInt(2))
            .putMyMap("bar", new OtherMessage().setMyInt(3))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putPrimitivesMap(1, false);
        Bar barCopy = bar.copy();
        assertEquals(bar, barCopy);
        assertEquals(bar.hashCode(), barCopy.hashCode());

        final Map<Bar, Integer> barMap = new HashMap<>();
        barMap.put(bar, 3);
        assertEquals(1, barMap.size());
        assertTrue(barMap.containsKey(barCopy));
        assertEquals(3, (int)barMap.get(bar));
        assertEquals(3, (int)barMap.get(barCopy));

        bar.setIntVariant(1);
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setBlah(new BlahPOJO().setStr("foo"));
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setStringVariant("foo");
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setBoolVariant(false);
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setFloatVariant(4.2f);
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.setLongVariant(123456L);
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearMyMap();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearRepeatedBlah();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearRepeatedBool();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearRepeatedInt();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
        barCopy = bar.copy();

        bar.clearRepeatedBytes();
        assertNotEquals(barCopy.hashCode(), bar.hashCode());
    }

    /**
     * testEquals.
     */
    @Test
    public void testEquals() {
        final Bar bar = new Bar()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahPOJO().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .setDoubleVariant(3.0)
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putMyMap("foo", new OtherMessage().setMyInt(2))
            .putMyMap("bar", new OtherMessage().setMyInt(3))
            .putPrimitivesMap(1, false);
        Bar barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setIntVariant(1);
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setBlah(new BlahPOJO().setStr("foo"));
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setStringVariant("foo");
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setBoolVariant(false);
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setFloatVariant(4.2f);
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.setLongVariant(123456L);
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearMyMap();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearRepeatedBlah();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearRepeatedBool();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearRepeatedInt();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);

        bar.clearRepeatedBytes();
        assertNotEquals(barCopy, bar);
        barCopy = bar.copy();
        assertEquals(bar, barCopy);
    }

    /**
     * testCopy.
     */
    @Test
    public void testCopy() {
        final Bar bar = new Bar()
            .setRequiredFloat(1.23f)
            .addAllRepeatedInt(Arrays.asList(1, 2, 3, 4))
            .addRepeatedBlah(new BlahPOJO().setStr("foo"))
            .addAllRepeatedBool(Arrays.asList(true, false, true))
            .addAllRepeatedString(Arrays.asList("foo", "bar", "baz"))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putMyMap("foo", new OtherMessage().setMyInt(2))
            .putMyMap("bar", new OtherMessage().setMyInt(3))
            .addAllRepeatedBytes(Arrays.asList(fooBytes, barBytes))
            .putPrimitivesMap(1, false);

        assertEquals(bar, bar.copy());

        bar.setBoolVariant(false);
        assertEquals(bar, bar.copy());

        bar.setGardenPlant(Plant.MAPLE);
        assertEquals(bar, bar.copy());

        bar.setMyBoolean(true);
        assertEquals(bar, bar.copy());

        bar.clearMyBoolean();
        assertEquals(bar, bar.copy());

        bar.setBlah(new BlahPOJO().setStr("blah"));
        assertEquals(bar, bar.copy());

        bar.clearBlah();
        assertEquals(bar, bar.copy());

        bar.clearRepeatedBool();
        assertEquals(bar, bar.copy());

        bar.clearRepeatedBytes();
        assertEquals(bar, bar.copy());

        final Bar empty = new Bar();
        assertEquals(empty, empty.copy());
    }
}
