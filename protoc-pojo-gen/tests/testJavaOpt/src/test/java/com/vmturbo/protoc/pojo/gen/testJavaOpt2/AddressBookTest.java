package com.vmturbo.protoc.pojo.gen.testjavaopt2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import com.vmturbo.protoc.pojo.gen.addresses.Addresses;
import com.vmturbo.protoc.pojo.gen.addresses.AddressesPOJO.PersonImpl;

/**
 * Tests for code generated for address book proto.
 */
public class AddressBookTest {
    /**
     * testCopy.
     */
    @Test
    public void testCopy() {
        final PersonImpl person = new PersonImpl().setId(2).setEmail("person@email.com");
        final PersonImpl copy = person.copy();

        assertEquals(2, copy.getId());
        assertEquals("person@email.com", copy.getEmail());
        assertEquals(person, copy);
        assertEquals(person.hashCode(), copy.hashCode());
    }

    /**
     * testCopyConstructor.
     */
    @Test
    public void testCopyConstructor() {
        final PersonImpl person = new PersonImpl().setId(2).setEmail("person@email.com");
        final PersonImpl copy = new PersonImpl(person);

        assertEquals(2, copy.getId());
        assertEquals("person@email.com", copy.getEmail());
        assertEquals(person, copy);
        assertEquals(person.hashCode(), copy.hashCode());
    }

    /**
     * testEquality.
     */
    @Test
    public void testEquality() {
        final PersonImpl person1 = new PersonImpl().setId(1).setEmail("person1@email.com");
        final PersonImpl person2 = new PersonImpl().setId(2).setEmail("person2@email.com");
        final PersonImpl identicalToPerson1 = new PersonImpl().setId(1).setEmail("person1@email.com");

        assertNotEquals(person1, person2);
        assertEquals(person1, identicalToPerson1);
    }

    /**
     * testToProto.
     */
    @Test
    public void testToProto() {
        final PersonImpl pojo = new PersonImpl().setId(1).setEmail("person@email.com");
        assertEquals(
            Addresses.Person.newBuilder()
                .setId(1)
                .setEmail("person@email.com")
                .build(),
            pojo.toProto());
    }

    /**
     * testFromProto.
     */
    @Test
    public void testFromProto() {
        final Addresses.Person.Builder builder = Addresses.Person.newBuilder()
            .setId(1)
            .setEmail("person@email.com");

        assertEquals(PersonImpl.fromProto(builder), PersonImpl.fromProto(builder.build()));
    }

    /**
     * testToByteArray.
     */
    @Test
    public void testToByteArray() {
        final Addresses.Person proto = Addresses.Person.newBuilder()
            .setId(1)
            .setEmail("person@email.com")
            .build();
        final PersonImpl pojo = new PersonImpl().setId(1).setEmail("person@email.com");

        assertArrayEquals(proto.toByteArray(), pojo.toByteArray());
    }
}
