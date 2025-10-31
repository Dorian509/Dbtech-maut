package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;

public class MautVerwaltungImpl implements IMautVerwaltung {

    private static final Logger L = LoggerFactory.getLogger(MautVerwaltungImpl.class);
    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }
    @Override
    public String getStatusForOnBoardUnit(long fz_id) {
        final String sql = "SELECT LOWER(TRIM(STATUS)) AS S FROM FAHRZEUGGERAT WHERE FZ_ID = ? OR FZG_ID = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setLong(1, fz_id);
            ps.setLong(2, fz_id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("S");
            }
        } catch (SQLException e) {
            throw new DataException("getStatusForOnBoardUnit failed for FZ_ID=" + fz_id, e);
        }
        return null;
    }

    @Override
    public int getUsernumber(int maut_id) {
        final String sql = """
        SELECT f.NUTZER_ID
        FROM   MAUTERHEBUNG   m
        JOIN   FAHRZEUGGERAT  g ON g.FZG_ID = m.FZG_ID
        JOIN   FAHRZEUG       f ON f.FZ_ID  = g.FZ_ID
        WHERE  m.MAUT_ID = ?
        """;
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setInt(1, maut_id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt("NUTZER_ID") : 0;
            }
        } catch (SQLException e) {
            throw new DataException("getUsernumber failed for MAUT_ID=" + maut_id, e);
        }
    }
    @Override
    public void registerVehicle(long fz_id, int sskl_id, int nutzer_id, String kennzeichen, String fin,
                                int achsen, int gewicht, String zulassungsland) {
        final String sql = """
            INSERT INTO FAHRZEUG
                (FZ_ID, SSKL_ID, NUTZER_ID, KENNZEICHEN, FIN, ACHSEN, GEWICHT, ZULASSUNGSLAND)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setLong(1, fz_id);
            ps.setInt(2, sskl_id);
            ps.setInt(3, nutzer_id);
            ps.setString(4, kennzeichen);
            ps.setString(5, fin);
            ps.setInt(6, achsen);
            ps.setInt(7, gewicht);
            ps.setString(8, zulassungsland);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DataException("registerVehicle failed for FZ_ID=" + fz_id, e);
        }
    }
    @Override
    public void updateStatusForOnBoardUnit(long fz_id, String status) {
        final String sql = "UPDATE FAHRZEUGGERAT SET STATUS = ? WHERE FZ_ID = ? OR FZG_ID = ?";
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, fz_id);
            ps.setLong(3, fz_id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DataException("updateStatusForOnBoardUnit failed for FZ_ID=" + fz_id, e);
        }
    }
    @Override
    public void deleteVehicle(long fz_id) {
        try {
            try (PreparedStatement ps = getConnection().prepareStatement(
                    "DELETE FROM POSITION WHERE MAUT_ID IN (SELECT MAUT_ID FROM MAUTERHEBUNG WHERE FZG_ID = ?)")) {
                ps.setLong(1, fz_id);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "DELETE FROM MAUTERHEBUNG WHERE FZG_ID = ?")) {
                ps.setLong(1, fz_id);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "DELETE FROM FAHRZEUGGERAT WHERE FZ_ID = ? OR FZG_ID = ?")) {
                ps.setLong(1, fz_id);
                ps.setLong(2, fz_id);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = getConnection().prepareStatement(
                    "DELETE FROM FAHRZEUG WHERE FZ_ID = ?")) {
                ps.setLong(1, fz_id);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DataException("deleteVehicle failed for FZ_ID=" + fz_id, e);
        }
    }
    @Override
    public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
        final String sql = """
            SELECT ABSCHNITTS_ID,
                   LAENGE,
                   START_KOORDINATE,
                   ZIEL_KOORDINATE,
                   NAME,
                   ABSCHNITTSTYP
            FROM   MAUTABSCHNITT
            WHERE  UPPER(ABSCHNITTSTYP) = UPPER(?)
            ORDER  BY ABSCHNITTS_ID
            """;
        List<Mautabschnitt> result = new ArrayList<>();
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, abschnittstyp);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new Mautabschnitt(
                            rs.getInt("ABSCHNITTS_ID"),
                            rs.getInt("LAENGE"),
                            rs.getString("START_KOORDINATE"),
                            rs.getString("ZIEL_KOORDINATE"),
                            rs.getString("NAME"),
                            rs.getString("ABSCHNITTSTYP")
                    ));
                }
            }
        } catch (SQLException e) {
            throw new DataException("getTrackInformations failed for type=" + abschnittstyp, e);
        }
        return result;
    }
}
