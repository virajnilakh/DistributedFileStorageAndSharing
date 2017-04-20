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
        	conn.setAutoCommit(false);
            String query = "insert into fileData (file_id, file_name, file_ext, num_of_chunks) values (?,?,?,?)";
            PreparedStatement preparedStatement = conn.prepareStatement( query );
            preparedStatement.setString( 1, fileid);
            preparedStatement.setString( 2, filename);
            preparedStatement.setString( 3, fileext);
            preparedStatement.setInt( 4, numOfChunks);
            preparedStatement.execute();
            preparedStatement.close();
            
            while(numOfChunks > 0){
	            String query2 = "insert into chunkData (file_id, chunk_id, chunk_data, chunk_size) values (?,?,?,?)";
	            PreparedStatement preparedStatement2 = conn.prepareStatement( query2 );
	            preparedStatement2.setString( 1, fileid);
	            preparedStatement2.setInt( 2, chunkid);
	            preparedStatement2.setBytes( 3, chunkdata);
	            preparedStatement2.setInt( 4, chunksize);
	            preparedStatement2.execute();
	            preparedStatement2.close();
	            numOfChunks -= 1;
	            if(numOfChunks == 0){
	            	conn.commit();
	            }
             }
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
	public int getChunk(String filename) {
		DBManager file_name = new DBManager();
        try {
            String query = "select num_of_chunks from fileData where file_name=?";
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

