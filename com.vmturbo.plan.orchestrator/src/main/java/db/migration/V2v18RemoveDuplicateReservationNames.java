package db.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

/**
 * remove the duplicate reservation name by suffixing it with __[1], __[2] ...
 * Also enforce unique constraint after de-duplication.
 */
public class V2v18RemoveDuplicateReservationNames implements JdbcMigration, MigrationInfoProvider {

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public void migrate(final Connection connection) throws Exception {
        try (PreparedStatement stmt = connection.prepareStatement(
                "UPDATE reservation SET name=? WHERE id=?")) {
            ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT id, name FROM reservation ORDER BY name");
            String previousName = null;
            int count = 0;
            while (rs.next()) {
                long reservationId = rs.getLong("id");
                String reservationName = rs.getString("name");
                if (previousName != null && reservationName.equals(previousName)) {
                    stmt.setString(1, reservationName + "___["
                            + count + "]");
                    stmt.setLong(2, reservationId);
                    count++;
                    stmt.execute();
                } else {
                    count = 0;
                }
                previousName = reservationName;
            }
            final String addUniqueConstraint = String.format("ALTER TABLE reservation "
                    + "ADD CONSTRAINT reservation_name_unique UNIQUE (name);");
            connection.createStatement().execute(addUniqueConstraint);
        } catch (SQLException e) {
            logger.warn("Failed performing migration", e);
            throw e;
        }
    }

    @Override
    public MigrationVersion getVersion() {
        return MigrationVersion.fromVersion("2.18");
    }

    @Override
    public String getDescription() {
        return "Remove duplicate reservation names";
    }
}