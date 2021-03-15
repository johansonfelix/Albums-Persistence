package DAO;

import pojo.Album;

import javax.sql.rowset.serial.SerialBlob;
import java.io.Serializable;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DatabaseManager implements Serializable {
    private final String host = "remotemysql.com";
    private final int portNum = 3306;
    private final String database = "6lTdvKVhWe";
    private final String uri = host + ":" + portNum+"/"+database;
    private final String username = "6lTdvKVhWe";
    private final String password = "gW9fmOuSij";

    private static DatabaseManager databaseManagerInstance;
    private Connection connection = null;
    public enum OperationType {
        INSERT,
        UPDATE
    }
    private String idColName = "id", timestampColName = "Timestamp", typeOfChangeColName = "Type of change", ISRCColName = "ISRC";
    private String tableName = "LogEntries";

    /**
     * We use the Table Data Gateway architectural pattern.
     * This class is a singleton and is responsible for querying and updating the database.
     */
    private DatabaseManager() {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your MySQL JDBC Driver?");
            e.printStackTrace();
            return;
        }

        System.out.println("MySQL JDBC Driver Registered!");
        try {
            connection = DriverManager
                    .getConnection("jdbc:mysql://" + uri, username, password);

        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

        if (connection != null) {
            System.out.println("Connected!");
        } else {
            System.out.println("Failed to make connection!");
        }

    }



    public static DatabaseManager getInstance() {

        if(databaseManagerInstance == null)
            databaseManagerInstance = new DatabaseManager();

        return databaseManagerInstance;
    }
    public pojo.LogEntry getLogEntry(int logID) {
        if (connection == null) {
            return null;
        } else {
            String query = "SELECT * FROM LogEntries WHERE id=" + logID;
            ResultSet results = executeQuery(query);
            ArrayList<pojo.LogEntry> entries = getLogEntriesFromResultSet(results);
            if (entries.size() != 1) {
                System.out.println("There are no entries matching that ID");
                return null;
            } else return entries.get(0);
        }
    }

    public ArrayList<pojo.LogEntry> getAllLogEntries() {
        if (connection == null) {
            return null;
        } else {
            String query = "SELECT * FROM LogEntries";
            ResultSet results = executeQuery(query);
            ArrayList<pojo.LogEntry> entries = getLogEntriesFromResultSet(results);
            if (entries.size() == 0) {
                System.out.println("There are no entries.");
                return null;
            } else return entries;
        }
    }

    public Album getAlbum(String ISRC) {
        System.out.println("Retrieving album with ISRC " + ISRC);
        if (connection == null) {
            System.out.println("No connection. Aborting");
            return null;
        } else {
            String query = "SELECT * FROM Albums WHERE ISRC=" + ISRC;
            ResultSet results = executeQuery(query);
            ArrayList<Album> albums = getAlbumsFromResultSet(results);
            if (albums.size() == 0) {
                System.out.println("There are no albums.");
                return null;
            } else {
                System.out.println("Found album: " + albums.get(0));
                return albums.get(0);
            }
        }
    }

    public ArrayList<Album> getAllAlbums() {
        System.out.println("Retrieving all albums");
        if (connection == null) {
            System.out.println("No connection. Aborting");
            return null;
        } else {
            String query = "SELECT * FROM Albums";
            ResultSet results = executeQuery(query);
            ArrayList<Album> albums = getAlbumsFromResultSet(results);
            if (albums.size() == 0) {
                System.out.println("There are no albums.");
                return null;
            } else {
                System.out.println("Retrieved " + albums.size() + " albums.");
                return albums;
            }
        }
    }

    private ArrayList<Album> getAlbumsFromResultSet(ResultSet results) {
        ArrayList<pojo.Album> albums = new ArrayList<>();
        try {
            while (results.next()) {
                pojo.Album album = new pojo.Album();
                album.setISRC(results.getString("ISRC"));
                album.setDescription(results.getString("Description"));
                album.setTitle(results.getString("Title"));
                album.setReleaseYear(results.getString("Release_Year"));
                album.setArtistFirstName(results.getString("Artist_First_Name"));
                album.setArtistLastName(results.getString("Artist_Last_Name"));
                Blob blob  = results.getBlob("Cover_Image");
                int blobLength = (int)blob.length();
                byte[] blobAsBytes = blob.getBytes(1,blobLength);
                album.setCover_img(blobAsBytes);
                albums.add(album);
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return albums;
    }

    private ArrayList<pojo.LogEntry> getLogEntriesFromResultSet(ResultSet results) {
        ArrayList<pojo.LogEntry> logEntries = new ArrayList<>();
        try {
            while (results.next()) {
                pojo.LogEntry logEntry = new pojo.LogEntry();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                Date parsedDate = dateFormat.parse(results.getString(timestampColName));
                logEntry.setId(results.getInt("ID"));
                logEntry.setTimestamp(new Timestamp(parsedDate.getTime()));
                logEntry.setT(pojo.LogEntry.stringToTypeOfChange(results.getString(typeOfChangeColName)));
                logEntry.setISRC(results.getString(ISRCColName));
                logEntries.add(logEntry);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return logEntries;
    }

    private ResultSet executeQuery(String query) {
        System.out.println("Executing query: " + query);
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            System.out.println("Results: " + rs.toString());
            return rs;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }
    public int insertOrUpdate(OperationType operation, String tableName, String[] colNames, Object[] values) {
        if (connection == null) {
            return -5;
        }
        try {
            String colNamesPart = "";
            String colValuesPart = "";
            for (int i = 0; i < colNames.length; i++) {
                colNamesPart += colNames[i];
                colValuesPart += "?";
                if (i + 1 < colNames.length) {
                    colNamesPart += ",";
                    colValuesPart += ",";
                }
            }
            String sql = "";
            if (operation == OperationType.INSERT) {
                sql = "INSERT INTO " + tableName + " (" + colNamesPart + ") VALUES (" + colValuesPart + ")";
            } else if (operation == OperationType.UPDATE) {
                sql = "UPDATE " + tableName + " SET ";
                for (int i = 0; i < colNames.length; i++) {
                    sql += colNames[i] + "=?";
                    if (i + 1 < colNames.length) {
                        sql += ",";
                    }
                }
            }
            PreparedStatement stmt = connection.prepareStatement(sql);

            for (int i = 0; i < values.length; i++) {
                if (values[i] instanceof String) {

                    stmt.setString(i + 1, (String) values[i]);
                } else if (values[i] instanceof SerialBlob) {
                    stmt.setBlob(i + 1, (SerialBlob) values[i]);
                }
            }
            System.out.println(stmt.toString());
            return stmt.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return -1;
    }

    public int delete(String tableName, String primaryKeyColName, Object primaryKeyValue) {
        String whereClauseRHS = "";
        if (primaryKeyValue instanceof Integer) {
            whereClauseRHS = ((Integer) primaryKeyValue).toString();
        } else if (primaryKeyValue instanceof String) {
            whereClauseRHS = (String) primaryKeyValue;
        }
        try {
            PreparedStatement stmt = connection.prepareStatement("DELETE FROM " + tableName + " WHERE " + primaryKeyColName + "=" + whereClauseRHS);
            return stmt.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return -1;
    }
}

