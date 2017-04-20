package database;

public class DBManager {
	private String fileId;
	private String filename;
    private String fileExt;
    private int chunkId;
    private int numOfChunks;
    private byte[] chunkData;
    private int chunkSize;
    
   
	public String getFileId() {
		return fileId;
	}


	public void setFileId(String fileId) {
		this.fileId = fileId;
	}


	public String getFilename() {
		return filename;
	}


	public void setFilename(String filename) {
		this.filename = filename;
	}


	public String getFileExt() {
		return fileExt;
	}


	public void setFileExt(String fileExt) {
		this.fileExt = fileExt;
	}


	public int getChunkId() {
		return chunkId;
	}


	public void setChunkId(int chunkId) {
		this.chunkId = chunkId;
	}


	public int getNumOfChunks() {
		return numOfChunks;
	}


	public void setNumOfChunks(int numOfChunks) {
		this.numOfChunks = numOfChunks;
	}


	public byte[] getChunkData() {
		return chunkData;
	}


	public void setChunkData(byte[] chunkData) {
		this.chunkData = chunkData;
	}


	public int getChunkSize() {
		return chunkSize;
	}


	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}
}
