import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Optional;

/**
 * @author xiaorui
 */
@Slf4j
public class AlbumDAO {
    private static final String DB_URL = "jdbc:postgresql://database-1.c93txhanfeao.us-west-2.rds.amazonaws.com:5432/album_db";
    private static final String DB_USER = "postgres";
    private static final String DB_PWD = "";
    private static HikariDataSource dataSource;

    static {
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(DB_URL);
            config.setUsername(DB_USER);
            config.setPassword(DB_PWD);

            config.setMaximumPoolSize(1000);

            config.setDriverClassName("org.postgresql.Driver");
            // Number of statement executions before preparing
            config.addDataSourceProperty("prepareThreshold", "5");
            // The number of prepared statements that the driver will cache per connection
            config.addDataSourceProperty("preparedStatementCacheQueries", "250");
            // The size of the prepared statement cache in megabytes (MiB)
            config.addDataSourceProperty("preparedStatementCacheSizeMiB", "5");

            // Minimum number of idle connections in the pool
            config.setMinimumIdle(50);
            // Idle timeout (10 minutes)
            config.setIdleTimeout(600000);
            // Max lifetime of a connection in the pool (30 minutes)
            config.setMaxLifetime(1800000);
            // Connection timeout (30 seconds)
            config.setConnectionTimeout(30000);

            dataSource = new HikariDataSource(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static Optional<Integer> saveAlbum(AlbumInfo albumInfo, byte[] imageBytes, Connection conn) {
        String insertAlbumSQL = "INSERT INTO albums.album(artist, title, year, image_data) VALUES(?,?,?,?)";

        try (PreparedStatement st = conn.prepareStatement(insertAlbumSQL, Statement.RETURN_GENERATED_KEYS)) {

            st.setString(1, albumInfo.getArtist());
            st.setString(2, albumInfo.getTitle());
            st.setString(3, albumInfo.getYear());
            st.setBytes(4, imageBytes);

            st.executeUpdate();

            try (ResultSet generatedKeys = st.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return Optional.of(generatedKeys.getInt(1));
                } else {
                    log.error("Failed to retrieve ID for the inserted album.");
                    return Optional.empty();
                }
            }

        } catch (SQLException ex) {
            log.error("Failed to save the album.", ex);
            return Optional.empty();
        }
    }

    public static AlbumInfo getAlbum(int albumId, Connection conn) throws SQLException {
        String getAlbumSQL = "SELECT artist, title, year FROM albums.album WHERE album_id = ?";

        try (PreparedStatement st = conn.prepareStatement(getAlbumSQL)) {

            st.setInt(1, albumId);

            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    return new AlbumInfo(rs.getString("artist"), rs.getString("title"), rs.getString("year"));
                }
            }
        } catch (SQLException ex) {
            log.error("Failed to retrieve the album with ID: " + albumId, ex);
        }
        return null;
    }
}
