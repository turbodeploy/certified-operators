package com.vmturbo.stitching;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityPropertyName;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;

public class StitchingMatchingMetaDataImplTest {
    private final static String STORAGE_ID = "StorageId";
    private final static String ID = "id";
    private final static String STORAGE_DATA = "storage_data";
    private final static String FILE = "file";
    private final static String DISPLAY_NAME = "displayName";
    private final static String FOO = "foo";
    private final static String BAR = "bar";

    private final EntityField files = EntityField.newBuilder()
            .addMessagePath(STORAGE_DATA)
            .setFieldName(FILE)
            .build();
    private final EntityField displayName = EntityField.newBuilder()
            .setFieldName(DISPLAY_NAME)
            .build();
    private final EntityPropertyName fooProperty = EntityPropertyName.newBuilder()
            .setPropertyName(FOO)
            .build();
    private final EntityPropertyName barProperty = EntityPropertyName.newBuilder()
            .setPropertyName(BAR)
            .build();

    private MergedEntityMetadata createMatchingMetadata() {
        EntityPropertyName storage = EntityPropertyName.newBuilder()
                .setPropertyName(STORAGE_ID)
                .build();
        MatchingData storageMatchingData = MatchingData.newBuilder()
                .setMatchingProperty(storage)
                .build();
        EntityField storageUuid = EntityField.newBuilder()
                .setFieldName(ID)
                .build();
        MatchingData storageUuidMatchingData = MatchingData.newBuilder()
                .setMatchingField(storageUuid)
                .build();
        MatchingMetadata storageMatchingMetadata = MatchingMetadata.newBuilder()
                .addMatchingData(storageMatchingData)
                .addExternalEntityMatchingProperty(storageUuidMatchingData)
                .build();
        final MergedEntityMetadata storageMergeEntityMetadata =
                MergedEntityMetadata.newBuilder().mergeMatchingMetadata(storageMatchingMetadata)
                        .addPatchedFields(files)
                        .addPatchedFields(displayName)
                        .addPatchedProperties(fooProperty)
                        .addPatchedProperties(barProperty)
                        .build();
        return storageMergeEntityMetadata;
    }

    @Test
    public void testEntityFieldsAndProperties() {
        final StitchingMatchingMetaData<String, String> matchingMetaData =
                new StitchingMatchingMetaDataImpl(EntityType.STORAGE, createMatchingMetadata()) {
            @Nonnull
            @Override
            public List<MatchingPropertyOrField<Collection<String>>> getInternalMatchingData() {
                return null;
            }

            @Nonnull
            @Override
            public List<MatchingPropertyOrField> getExternalMatchingData() {
                return null;
            }
        };

        assertEquals(Lists.newArrayList(FOO, BAR), matchingMetaData.getPropertiesToPatch());

        assertEquals(Lists.newArrayList(files.getFieldName(), displayName.getFieldName()),
                matchingMetaData.getAttributesToPatch()
                        .stream()
                        .map(DTOFieldSpec::getFieldName)
                        .collect(Collectors.toList()));
        assertEquals(Lists.newArrayList(STORAGE_DATA), matchingMetaData.getAttributesToPatch()
                .stream()
                .map(DTOFieldSpec::getMessagePath)
                .filter(list -> !list.isEmpty())
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
    }

}
