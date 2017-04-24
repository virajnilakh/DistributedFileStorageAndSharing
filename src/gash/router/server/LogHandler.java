package gash.router.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import global.Constants;

public class LogHandler {

	protected static AtomicReference<LogHandler> instance = new AtomicReference<LogHandler>();
	protected static FileWriter fw = null;
	protected static BufferedWriter bw = null;
	protected static PrintWriter pw = null;
	protected static int LamportCount;
	protected static Logger logger = LoggerFactory.getLogger("ReplicationLogCreator");

	protected LogHandler() throws IOException {
		// Init log file
		// this.File=file;
		// this.port = port;

		init();
	}

	// Init stuff
	private void init() throws IOException {
		// Init fileWriter with filePath
		File logFile = new File(Constants.logFile);
		logFile.createNewFile(); // Create log File

		fw = new FileWriter(logFile, true);
		bw = new BufferedWriter(fw);
		pw = new PrintWriter(bw);
		LamportCount = 0;
	}

	public static LogHandler getInstance() {
		// TODO throw exception if not initialized!
		return instance.get();
	}

	/**
	 * release all resources
	 */
	public void release() {
		instance = null;
	}

	// Create log
	public static Boolean createLog(String statement) {
		Boolean isUnLogged = true;
		try {

			pw.println(statement);
			LamportCount++;
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isUnLogged = false;
		}
		return true;
	}

	public static void tryLogCreate(String statement) {

		Boolean isCreated = true;
		// Try log Creation until infinity
		while (isCreated) {
			isCreated = LogHandler.createLog(statement);
		}
	}

}
