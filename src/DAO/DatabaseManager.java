package DAO;

import pojo.Album;

import javax.sql.rowset.serial.SerialBlob;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DatabaseManager {
    private final String host = "localhost";
    private final int portNum = 3306;
    private final String uri = host + ":" + portNum;
    private final String username = "root";
    private final String password = "";
    private static DatabaseManager databaseManagerInstance = new DatabaseManager();
    private Connection connection = null;
    private enum OperationType {
        INSERT,
        UPDATE
    }
    private String idColName = "id", timestampColName = "TStamp", typeOfChangeColName = "Type of change", ISRCColName = "ISRC";
    private String tableName = "LogEntries";

    /**
     * We use the Table Data Gateway architectural pattern.
     * This class is a singleton and is responsible for querying and updating the database.
     */
    private DatabaseManager() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
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
    public boolean haveConnection(){
        System.out.println("Performing database operation. ");
        System.out.println("CHECK: | Connected to database? " + (connection != null));
        return connection != null;
    }
    public ArrayList<pojo.LogEntry> getAllLogEntries() {
        if(haveConnection()){
            String query = "SELECT * FROM LogEntries";
            ResultSet results = executeQuery(query);
            ArrayList<pojo.LogEntry> entries = getLogEntriesFromResultSet(results);
            if (entries.size() == 0) {
                System.out.println("There are no entries.");
                return null;
            } else return entries;
        }
        return null;
    }

    public Album getAlbum(String ISRC) {
        System.out.println("Retrieving album with ISRC " + ISRC);
        if(haveConnection()){
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
        return null;
    }

    public ArrayList<Album> getAllAlbums() {
        System.out.println("Retrieving all albums");
        if (haveConnection()) {
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
        return null;
    }

    private ArrayList<Album> getAlbumsFromResultSet(ResultSet results) {
        ArrayList<pojo.Album> albums = new ArrayList<>();
        System.out.println("Getting albums from result set..");
        int numAlbums = 0;
        try {
            while (results.next()) {
                System.out.println("Fetching next result: " + results.toString());
                pojo.Album album = new pojo.Album();
                album.setISRC(results.getString("ISRC"));
                album.setDescription(results.getString("Description"));
                album.setTitle(results.getString("Ttile"));
                album.setReleaseYear(results.getString("Release-Year"));
                album.setArtistFirstName(results.getString("Artist-First-Name"));
                album.setArtistLastName(results.getString("Artist-Last-Name"));
                Blob blob  = results.getBlob("Cover-Image");
                int blobLength = (int)blob.length();
                byte[] blobAsBytes = blob.getBytes(1,blobLength);
                album.setCover_img(blobAsBytes);
                albums.add(album);
                numAlbums++;
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println("Finished fetching albums. total entries: " + numAlbums);
        return albums;
    }

    private ArrayList<pojo.LogEntry> getLogEntriesFromResultSet(ResultSet results) {
        System.out.println("Getting log entries from the result set..");
        ArrayList<pojo.LogEntry> logEntries = new ArrayList<>();
        int numLogEntries = 0;
        try {
            while (results.next()) {
                System.out.println("Fetched next result: " + results.toString());
                pojo.LogEntry logEntry = new pojo.LogEntry();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                Date parsedDate = dateFormat.parse(results.getString(timestampColName));
                logEntry.setId(results.getInt("ID"));
                logEntry.setTimestamp(new Timestamp(parsedDate.getTime()));
                logEntry.setT(pojo.LogEntry.stringToTypeOfChange(results.getString(typeOfChangeColName)));
                logEntry.setISRC(results.getString(ISRCColName));
                logEntries.add(logEntry);
                numLogEntries++;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println("Finished fetching log entries. Total entries: " + numLogEntries);
        return logEntries;
    }

    private ResultSet executeQuery(String query) {
        System.out.println("Executing query: \n" + query);
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

    private int insertOrUpdate(OperationType operation, String tableName, String[] colNames, Object[] values) {
        System.out.println("Called insertOrUpdate function");
        if(haveConnection()) {
            try {
                System.out.println("Constructing statement...");
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
                System.out.println("Statement to be executed:");
                System.out.println(sql);
                PreparedStatement stmt = connection.prepareStatement(sql);
                for (int i = 0; i < values.length; i++) {
                    if (values[i] instanceof String) {
                        stmt.setString(i + 1, (String) values[i]);
                    } else if (values[i] instanceof SerialBlob) {
                        stmt.setBlob(i + 1, (SerialBlob) values[i]);
                    }
                }
                System.out.println("Executing statement..");
                int rowsAffected = stmt.executeUpdate();
                System.out.println("Statement executed. Rows affected: " + rowsAffected);
                return rowsAffected;
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        return -1;
    }

    private int delete(String tableName, String primaryKeyColName, Object primaryKeyValue) {
        if(haveConnection()) {
            System.out.println("Deleting from " + tableName + "  where the column is " + primaryKeyColName + " and value looking for is " + primaryKeyColName);
            String whereClauseRHS = "";
            if (primaryKeyValue instanceof Integer) {
                System.out.println("The primary key is of type integer.");
                whereClauseRHS = ((Integer) primaryKeyValue).toString();
            } else if (primaryKeyValue instanceof String) {
                System.out.println("The primary key is of type String");
                whereClauseRHS = (String) primaryKeyValue;
            }
            try {
                String sql = "DELETE FROM " + tableName + " WHERE " + primaryKeyColName + "=" + whereClauseRHS;
                System.out.println("Executing update: \n" + sql);
                PreparedStatement stmt = connection.prepareStatement(sql);
                return stmt.executeUpdate();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        return -1;
    }
}