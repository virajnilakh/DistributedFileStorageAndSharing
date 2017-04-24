package database;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.google.protobuf.ByteString;

//import gash.router.server.LogHandler;

public class DBHandler {

	private Connection conn;

	public DBHandler() {
		conn = makeConn();
	}

	public Connection makeConn() {
		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/cmpe275db", "root", "test");
			System.out.println("Successfully connected");

		} catch (Exception e) {
			System.err.println("Got an exception! ");
			System.err.println(e.getMessage());
		}
		return conn;
	}

	public void addChunk(String fileid, int chunkid, byte[] chunkdata, int chunksize, int numOfChunks) {
		try {
			if (checkFileExists(fileid)) {
				deletePrevious(fileid);
			}
			String query2 = "insert into chunkData (file_id, chunk_id, chunk_data, chunk_size) values (?,?,?,?)";
			PreparedStatement preparedStatement2 = conn.prepareStatement(query2);
			preparedStatement2.setString(1, fileid);
			preparedStatement2.setInt(2, chunkid);
			preparedStatement2.setBytes(3, chunkdata);
			preparedStatement2.setInt(4, chunksize);
			preparedStatement2.execute();
			preparedStatement2.close();
			numOfChunks -= 1;

			//LogHandler.getInstance();
			//LogHandler.tryLogCreate(query2);

			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean checkFileExists(String fileid) {
		try {
			// if the file already exists, delete current entries and replace
			// with the new ones
			String existsQuery = "select * from filedata where file_id =?";
			PreparedStatement preparedStatement;
			preparedStatement = conn.prepareStatement(existsQuery);
			preparedStatement.setString(1, fileid);
			ResultSet resultSet = preparedStatement.executeQuery();
			if (resultSet.first()) {
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public void deletePrevious(String fileid) {
		try {
			// System.out.println("The file you're trying to write already
			// EXISTS in MySQL. Now the NEW would replace OLD!");
			String deleteQuery1 = "delete from filedata where file_id = ?";
			String deleteQuery2 = "delete from chunkdata where file_id =?";
			PreparedStatement preparedStatement2 = conn.prepareStatement(deleteQuery1); // delete
																						// from
																						// filedata
			preparedStatement2.setString(1, fileid);
			PreparedStatement preparedStatement3 = conn.prepareStatement(deleteQuery2); // delete
																						// from
																						// chunkdata
			preparedStatement3.setString(1, fileid);
			preparedStatement2.execute();
			preparedStatement3.execute();
			//LogHandler.getInstance();
			//LogHandler.tryLogCreate(deleteQuery1);
			//LogHandler.tryLogCreate(deleteQuery1);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void deletePreviousFile(String fileid) {
		try {
			String deleteQuery1 = "delete from filedata where file_id = ?";
			PreparedStatement preparedStatement2 = conn.prepareStatement(deleteQuery1); // delete
																						// from
																						// filedata
			preparedStatement2.setString(1, fileid);
			preparedStatement2.execute();
			//LogHandler.getInstance();
			//LogHandler.tryLogCreate(deleteQuery1);
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addFile(String fileid, String filename, String fileext, int numOfChunks, long filesize) {
		try {
			if (checkFileExists(fileid)) {
				deletePreviousFile(fileid);
			}
			conn.setAutoCommit(false);
			String query = "insert into fileData (file_id, file_name, file_ext, num_of_chunks, file_size) values (?,?,?,?,?)";
			PreparedStatement preparedStatement4 = conn.prepareStatement(query);
			preparedStatement4.setString(1, fileid);
			preparedStatement4.setString(2, filename);
			preparedStatement4.setString(3, fileext);
			preparedStatement4.setInt(4, numOfChunks);
			preparedStatement4.setLong(5, filesize);
			preparedStatement4.execute();
			preparedStatement4.close();
			//LogHandler.getInstance();
			//LogHandler.tryLogCreate(query);

			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public ArrayList<Chunk> getChunks(String fileid) {
		/* DBManager entireRow = new DBManager(); */
		ArrayList<Chunk> allChunks = new ArrayList<Chunk>();

		try {
			String query = "select * from chunkData where file_id=?";
			PreparedStatement preparedStatement = conn.prepareStatement(query);
			preparedStatement.setString(1, fileid);
			ResultSet resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				Chunk eachChunk = new Chunk();
				eachChunk.setChunkId(resultSet.getInt("chunk_id"));
				// int eachChunk =
				// ByteString.copyFrom(resultSet.getBytes("chunk_data"));
				eachChunk.setChunkData(resultSet.getBytes("chunk_data"));
				eachChunk.setChunkSize(resultSet.getInt("chunk_size"));
				allChunks.add(eachChunk);
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return allChunks;
	}

	public void closeConn() {
		if (conn == null)
			return;
		try {
			conn.close();
			System.out.println("Connection closed!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
