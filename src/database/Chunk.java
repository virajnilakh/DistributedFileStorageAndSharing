package database;

public class Chunk {
	private int chunkId;
    private byte[] chunkData;
    private int chunkSize;
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
    
    
}
