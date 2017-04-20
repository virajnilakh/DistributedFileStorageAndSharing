package database;
import java.sql.*;
	 
public class DBHandler {
 
    private Connection conn;
    
    public DBHandler() {
        conn = makeConn();
    }
    
    public Connection makeConn(){
    	try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cmpe275db","root", "root");
	        System.out.println("Successfully connected");

		}catch (Exception e) {
	        System.err.println("Got an exception! ");
	        System.err.println(e.getMessage());
	    }
    	return conn;
    }
 
    public void addChunk(String fileid, String filename, String fileext, int chunkid, int numOfChunks, byte[] chunkdata, int chunksize) {
        try {
            String query = "insert into filestorage (file_id, file_name, file_ext, chunk_id, num_of_chunks, chunk_data, chunk_size) values (?,?,?,?,?,?,?)";
            PreparedStatement preparedStatement = conn.prepareStatement( query );
            preparedStatement.setString( 1, fileid);
            preparedStatement.setString( 2, filename);
            preparedStatement.setString( 3, fileext);
            preparedStatement.setInt( 4, chunkid);
            preparedStatement.setInt( 5, numOfChunks );
            preparedStatement.setBytes( 6, chunkdata);
            preparedStatement.setInt( 7, chunksize);
            
            System.out.println("----> Wrote " + numOfChunks + " chunks for file = " + filename);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
	public int getChunk(String filename) {
		DBManager file_name = new DBManager();
        try {
            String query = "select * from filestorage where file_name=?";
            PreparedStatement preparedStatement = conn.prepareStatement( query );
            preparedStatement.setString(1, filename);
            ResultSet resultSet = preparedStatement.executeQuery();
            while( resultSet.next() ) {
            	file_name.setNumOfChunks(resultSet.getInt("num_of_chunks"));            	
            	System.out.println("-----> " + filename + " has " + resultSet.getString("num_of_chunks") + " chunks!");
            }
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return file_name.getNumOfChunks();
    }
		
    public void closeConnection() {
        if( conn == null )
            return;
        try {
            conn.close();
            System.out.println("Connection closed!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

