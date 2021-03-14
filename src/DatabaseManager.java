import pojo.Album;

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

    private String idColName = "id", timestampColName = "Timestamp", typeOfChangeColName = "Type of change", ISRCColName = "ISRC";
    private String tableName = "LogEntries";

    /**
     * We use the Table Data Gateway architectural pattern.
     * This class is a singleton and is responsible for querying and updating the database.
     */
    private DatabaseManager(){
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
                    .getConnection("jdbc:mysql://"+uri,username, password);

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
    public static DatabaseManager getInstance(){
        return databaseManagerInstance;
    }
    public pojo.LogEntry getLogEntry(int logID){
        if(connection == null){
            return null;
        }
        else {
            String query = "SELECT * FROM LogEntries WHERE id=" + logID;
            ResultSet results = executeQuery(query);
            ArrayList<pojo.LogEntry> entries = getLogEntriesFromResultSet(results);
            if(entries.size() != 1){
                System.out.println("There are no entries matching that ID");
                return null;
            }
            else return entries.get(0);
        }
    }
    public ArrayList<pojo.LogEntry> getAllLogEntries(){
        if(connection == null){
            return null;
        }
        else {
            String query = "SELECT * FROM LogEntries";
            ResultSet results = executeQuery(query);
            ArrayList<pojo.LogEntry> entries = getLogEntriesFromResultSet(results);
            if(entries.size() == 0){
                System.out.println("There are no entries.");
                return null;
            }
            else return entries;
        }
    }
    public Album getAlbum(String ISRC){
        System.out.println("Retrieving album with ISRC " + ISRC);
        if(connection == null){
            System.out.println("No connection. Aborting");
            return null;
        }
        else{
            String query = "SELECT * FROM Albums WHERE ISRC=" + ISRC;
            ResultSet results = executeQuery(query);
            ArrayList<Album> albums = getAlbumsFromResultSet(results);
            if(albums.size() == 0) {
                System.out.println("There are no albums.");
                return null;
            } else {
                System.out.println("Found album: "+ albums.get(0));
                return albums.get(0);
            }
        }
    }
    public ArrayList<Album> getAllAlbums(){
        System.out.println("Retrieving all albums");
        if(connection == null){
            System.out.println("No connection. Aborting");
            return null;
        }
        else{
            String query = "SELECT * FROM Albums";
            ResultSet results = executeQuery(query);
            ArrayList<Album> albums = getAlbumsFromResultSet(results);
            if(albums.size() == 0) {
                System.out.println("There are no albums.");
                return null;
            } else {
                System.out.println("Retrieved " + albums.size() + " albums.");
                return albums;
            }
        }
    }
    private ArrayList<Album> getAlbumsFromResultSet(ResultSet results){
        ArrayList<pojo.Album> albums = new ArrayList<>();
        try {
            while(results.next()){
                pojo.Album album = new pojo.Album();
                album.setISRC(results.getString("ISRC"));
                album.setDescription(results.getString("Description"));
                album.setTitle(results.getString("Ttile"));
                album.setReleaseYear(results.getString("Release-Year"));
                album.setArtistFirstName(results.getString("Artist-First-Name"));
                album.setArtistLastName(results.getString("Artist-Last-Name"));
                albums.add(album);
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return albums;
    }
    private ArrayList<pojo.LogEntry> getLogEntriesFromResultSet(ResultSet results){
        ArrayList<pojo.LogEntry> logEntries = new ArrayList<>();
        try {
            while(results.next()){
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
    public String insertAlbum(Album album){
        System.out.println("Attempting to insert a new album: " + album.toString());
        if(connection == null){
            return "No connection. Aborting..";
        }
        else{
            String query = "INSERT INTO Albums VALUES (" +
                    album.getISRC() + "," +
                    album.getTitle() + "," +
                    album.getDescription() + "," +
                    album.getReleaseYear() + "," +
                    album.getArtistFirstName() + "," +
                    album.getArtistLastName();
            query += ")";
            executeUpdate(query);
            System.out.println("Inserted album.");
            return "Inserted album.";
        }

    }
    public String updateAlbum(Album album){
        System.out.println("Attempting to update album..");
        if(connection == null){
            return "Connection not set. Aborting.";
        }
        String query = "UPDATE Albums SET ";
        query += "ISRC" + "=" + album.getISRC() + ",";
        query += "Title" + "=" + album.getTitle() + ",";
        query += "Description" + "=" + album.getDescription() + ",";
        query += "Release-Year" + "=" + album.getReleaseYear() + ",";
        query += "Artist-First-Name" + "=" + album.getArtistFirstName() + ",";
        query += "Artist-Last-Name" + "=" + album.getArtistLastName();
        query += " WHERE ISRC=" + album.getISRC();
        executeQuery(query);
        return "Updated album.";
    }
    public String deleteAlbum(String ISRC){
        System.out.println("Attempting to delete album with ISRC: " + ISRC);
        if(connection == null){
            return "No connection. Aborting.";
        }
        else{
            String query = "DELETE FROM Albums WHERE ISRC=" + ISRC;
            executeUpdate(query);
            System.out.println("Album " + ISRC + " deleted.");
            return "Deleted.";
        }
    }
    public String insertLogEntry(pojo.LogEntry logEntry){
        System.out.println("Attempting to insert log entry: " + logEntry.toString());
        if(connection == null){
            return "Connection not set. Aborting.";
        }
        else{
            String query = "INSERT INTO LogEntries VALUES (" +
                    logEntry.getTimestamp().toString() + "," +
                    pojo.LogEntry.typeOfChangeToString(logEntry.getT()) + "," +
                    logEntry.getISRC();
            query += ")";
            executeUpdate(query);
            System.out.println("Inserted log entry.");
            return "Done inserting log entry.";
        }
    }
    public String updateLogEntry(pojo.LogEntry logEntry){
        System.out.println("Attempting to update log entry..");
        if(connection == null){
            return "Connection not set. Aborting.";
        }
        String query = "UPDATE LogEntries SET ";
        query += timestampColName + "=" + logEntry.getTimestamp().toString() + ",";
        query += typeOfChangeColName + "=" + pojo.LogEntry.typeOfChangeToString(logEntry.getT()) + ",";
        query += ISRCColName + "=" + logEntry.getISRC();
        query += " WHERE id=" + logEntry.getId();
        executeUpdate(query);
        System.out.println("Updated log entry with ID " + logEntry.getId());
        return "Updated log entry.";
    }
    private ResultSet executeQuery(String query){
        System.out.println("Executing query: " + query);
        try{
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            System.out.println("Results: " + rs.toString());
            return rs;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }
    private int executeUpdate(String update){
        System.out.println("Executing update: " + update);
        if(connection == null){
            System.out.println("Connection not set. Aborting.");
            return -1;
        }
        int rowsAffected = -5;
        try{
            Statement stmt = connection.createStatement();
            rowsAffected = stmt.executeUpdate(update);
            System.out.println("Finished updating. Rows affected: " + rowsAffected);
            return rowsAffected;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return rowsAffected;
    }
}
