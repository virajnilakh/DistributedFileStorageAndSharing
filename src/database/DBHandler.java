package database;
import java.sql.*;
import java.util.ArrayList;

import com.google.protobuf.ByteString;
	 
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
    
	public ArrayList<ByteString> getChunks(String fileid) {
        ArrayList<ByteString> allChunks = new ArrayList<ByteString>();
		try {
            String query = "select * from chunkData where file_id=?";
            PreparedStatement preparedStatement = conn.prepareStatement( query );
            preparedStatement.setString(1, fileid);
            ResultSet resultSet = preparedStatement.executeQuery();

            while( resultSet.next() ) {
            	ByteString eachChunk = ByteString.copyFrom(resultSet.getBytes("chunk_data"));
            	allChunks.add(eachChunk);
            }
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return allChunks;
    }
		
    public void closeConn() {
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

