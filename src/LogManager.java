import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class LogManager {
    private final String host = "localhost";
    private final int portNum = 3306;
    private final String uri = host + ":" + portNum;
    private static LogManager logManagerInstance = new LogManager();
    private Connection connection = null;
    private String username = "root";
    private String password = "";
    private String idColName = "id", timestampColName = "Timestamp", typeOfChangeColName = "Type of change", ISRCColName = "ISRC";
    private String tableName = "LogEntries";

    /**
     * We use the Table Data Gateway architectural pattern.
     */
    private LogManager(){
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
                    .getConnection("jdbc:mysql://"+uri,"root", "password");

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
    public static LogManager getInstance(){
        return logManagerInstance;
    }
    public pojo.LogEntry findLogEntry(int logID){
        if(connection == null){
            return null;
        }
        else {
            String query = "SELECT * FROM LogEntries WHERE id=" + logID;
            ResultSet results = execute(query);
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
            ResultSet results = execute(query);
            ArrayList<pojo.LogEntry> entries = getLogEntriesFromResultSet(results);
            if(entries.size() == 0){
                System.out.println("There are no entries.");
                return null;
            }
            else return entries;
        }
    }

    private ArrayList<pojo.LogEntry> getLogEntriesFromResultSet(ResultSet results){
        ArrayList<pojo.LogEntry> logEntries = new ArrayList<>();
        try {
            while(results.next()){
                pojo.LogEntry logEntry = new pojo.LogEntry();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                Date parsedDate = dateFormat.parse(results.getString(timestampColName));
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
        return null;
    }
    public boolean insertLogEntry(pojo.LogEntry logEntry){
        if(connection == null){
            return false;
        }
        else{
            String query = "INSERT INTO LogEntries VALUES (" +
                    logEntry.getTimestamp().toString() + "," +
                    pojo.LogEntry.typeOfChangeToString(logEntry.getT()) + "," +
                    logEntry.getISRC();
            query += ")";
            return true;
        }
    }
    public boolean updateLogEntry(int logID, pojo.LogEntry logEntry){
        String query = "UPDATE LogEntries SET ";
        query += timestampColName + "=" + logEntry.getTimestamp().toString() + ",";
        query += typeOfChangeColName + "=" + pojo.LogEntry.typeOfChangeToString(logEntry.getT()) + ",";
        query += ISRCColName + "=" + logEntry.getISRC();
        query += " WHERE id=" + logID;
        execute(query);
        return true;
    }
    private ResultSet execute(String query){
        try{
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            return rs;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }
}
