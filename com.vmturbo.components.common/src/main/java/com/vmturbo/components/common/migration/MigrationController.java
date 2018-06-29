package com.vmturbo.components.common.migration;

import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import com.vmturbo.common.protobuf.common.Migration.MigrationRecord;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * REST Endpoint for Migrations.
 */
@Api(value = "/migration")
@RestController
@RequestMapping(path = "/migration")
public class MigrationController {

    final MigrationFramework dataMigrations;

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    public MigrationController(@Nonnull final MigrationFramework dataMigrations) {
        this.dataMigrations = dataMigrations;
    }

    @RequestMapping(method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation("Return the list of migrations.")
    public ResponseEntity<String> listMigrations() {
        return ResponseEntity.status(HttpStatus.OK)
                .body(GSON.toJson(dataMigrations.listMigrations()
                        .stream()
                        .map(rec -> GSON.toJson(rec, MigrationRecord.class))
                        .collect(Collectors.toList())));
    }

    @RequestMapping(value="/{migrationName}",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation("Return the migration info.")
    public ResponseEntity<String> getMigration(
            @PathVariable("migrationName") final String migrationName) {
        Optional<MigrationRecord> record = dataMigrations.getMigrationRecord(migrationName);
        if (record.isPresent()) {
            return ResponseEntity.status(HttpStatus.OK)
                .body(GSON.toJson(record.get(), MigrationRecord.class));
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).
                        body(GSON.toJson(MigrationRecord.getDefaultInstance(),
                                MigrationRecord.class));
        }
    }
}
