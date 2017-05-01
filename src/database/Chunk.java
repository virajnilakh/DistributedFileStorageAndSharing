package database;

public class Chunk {
	private int chunkId;
	private byte[] chunkData;
	private int chunkSize;
	private String fileId;

	public int getChunkId() {
		return chunkId;
	}

	public void setChunkId(int chunkId) {
		this.chunkId = chunkId;
	}

	public byte[] getChunkData() {
		return chunkData;
	}

	public void setChunkData(byte[] bs) {
		this.chunkData = bs;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public String getFileName(String FileId, DBHandler handler) {

		return handler.getFileName(fileId);

	}

	public void setChunkFileId(String fileId) {

		this.fileId = fileId;
	}

	public String getChunkFileId() {

		return fileId;
	}
}