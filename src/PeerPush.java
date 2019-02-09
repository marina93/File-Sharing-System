import java.net.*;
import java.util.ArrayList;
//import java.util.ArrayList;
import java.io.*;

/**
 * @author jcerecedameca@hawk.iit.edu - A20432616
 * @author mtorresgomez@hawk.iit.edu - A20432664
 * 
 *         Class Peer. This class contains all the necessary methods to create a
 *         Peer and to execute it. The main functions of it as client are
 *         registry, search and retrieve. As a server, it will create socket and
 *         listen on it for any file request.
 * 
 */
public class PeerPush {

	private static final int TTL1 = 15;
	private static final int TTL2 = 15;
	private int messID1;
	private int messID2;
	private int port;
	private String dir1;
	private String dir2;
	private int serverPort;

	/**
	 * Constructor of the class. It will identify a single Peer and start its Server
	 * side.
	 * 
	 * @param dir  Directory in which the files of the peer are located.

	 * @param port Port in which the ServerSocket of the Peer is going to be
	 *             listening.
	 */
	
	public PeerPush(String dir1, String dir2, int port) {
		this.dir1 = dir1;
		this.dir2 = dir2;
		this.port = port;
		this.messID1 = 0;
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					startClientServer(port);
				}
			}
		}).start();
		
	}
	

	/**
	 * Getter of the File Directory.
	 * 
	 * @return String containing the name of the File directory.
	 */
	public String getDir1() {
		return this.dir1;
	}
	
	/**
	 * Getter of the File Directory.
	 * 
	 * @return String containing the name of the File directory.
	 */
	public String getDir2() {
		return this.dir2;
	}

	/**
	 * Getter of the Peer's listening Port.
	 * 
	 * @return Integer containing the listening Port of the Peer
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * Setter for changing the serverPort attribute.
	 * 
	 * @param serverPort
	 */
	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}
	
	public boolean modifyFile(String fileName) {
		boolean check = new File(dir2, fileName).exists();
		// CAMBIAR, CONSULTAR PRIVATE
		if (check) {
			try {
				File file = new File(fileName);
				FileWriter fw = new FileWriter(file);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write("Some text");
				long version = file.lastModified();
				bw.flush();
				bw.close();
				invalidate(messID2,serverPort,fileName,port,TTL2, version);
				return true;
			} catch(IOException e) {
				e.printStackTrace();
				return false;
			}
		} else {
			System.out.println("Access denied");
			return false;
		}
		
	}
	/**
	 * Register function. This function is in charge of sending a message to the
	 * Indexing server in order to be registered on it.
	 * 
	 * @param portNumber - Indexing Server's port number.
	 */
	public void register(int portNumber) {
		PrintWriter out = null;
		BufferedReader in = null;
		Socket socket = null;
		try {
			socket = new Socket("localhost", portNumber);
			out = new PrintWriter(socket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out.println("registry%" + this.port +  "%" + this.dir1 + "%" + this.dir2);
			System.out.println("Message Received: " + in.readLine());
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}

	/**
	 * Retrieve function. This function is in charge of requesting another peer a
	 * file. It receives the file, stores its and sends an update to the indexing
	 * server.
	 * 
	 * @param fileName - Name of the requested file.
	 * @param port     - Port of the Peer that acts as a server.
	 * @throws Exception - Socket could not be opened.
	 */
	public void retrieve(String fileName, int port) throws Exception {
		try {
			
			
			Socket socket = new Socket("localhost", port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			out.println(fileName);
			BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dir1 + "/" + fileName));

			int i;
			byte[] receivedData = new byte[6022386];
			while ((i = bis.read(receivedData)) != -1) {
				bos.write(receivedData, 0, i);
			}
			bos.close();
			out.close();

			System.out.println("File " + fileName + " downloaded");

			register(serverPort);
			socket.close();
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}

	/**
	 * Search function. It makes a petition to the indexing server for a file and
	 * prints the response (Peers that have that file).
	 * 
	 * @param fileName - Name of the requested file.
	 */
	public void requestFile(String fileName) {
		PrintWriter out = null;
		Socket socket = null;
		int portNumber = this.serverPort;

		try {
			socket = new Socket("localhost", portNumber);
			out = new PrintWriter(socket.getOutputStream(), true);
			out.println("query%" + this.messID1 + "%" + TTL1 + "%" + fileName + "%" + this.port);
			out.close();
			socket.close();
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}
	public void invalidate(int messID, int severPort, String fileName, int port, int TTL, long version) {
		PrintWriter out = null;
		Socket socket = null;
		try {
			socket = new Socket("localhost", serverPort);
			out = new PrintWriter(socket.getOutputStream(), true);
			
			out.println("invalidate%" + this.messID2 + "%" + serverPort + "%" + fileName + "%" + port + "%" + TTL+ "%" + version);
			out.close();
			socket.close();
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}
	

	/**
	 * Function that shows the received queryHit from the SuperPeer with an answer.
	 * User then can decide which of the receiving answer to choose to retrieve the file.
	 * 
	 * @param s - The received string from the superPeer
	 */
//out.println("queryhit%" + messageID + "%15%" + fileName + "%" + result.get(i));

	private void queryHit(String s) {
		String fileName = s.split("%")[3];
		
		String sPort = s.split("%")[4];

		System.out.println("Filename " + fileName + " found in: " + sPort);
		System.out.println("query "+this.port+ " time: "+System.currentTimeMillis());
		System.out.print("Enter command:");
	}
	
	public ArrayList<String> listFilesFromDir(final File dir) {
		System.out.println("listFilesFromDir: "+ dir);

		ArrayList<String> fileNames = new ArrayList<String>();
		if(dir.listFiles()!=null) {
			for (final File archiveFile : dir.listFiles()) {
	
				fileNames.add(archiveFile.getName());
			}
		}	
			return fileNames;
	}
	
	public boolean deleteFile(final File dir, String fileName) {
		boolean deleted = false;
		ArrayList<String> fileNames = new ArrayList<String>();
		for (final File archiveFile : dir.listFiles()) {
			if(archiveFile.getName()==fileName)
			fileNames.remove(archiveFile.getName());
			deleted = true;
		}
		return true;
	}
	
	public boolean fileExists(final File dir, String fileName) {
		ArrayList<String> fileNames = new ArrayList<String>();
		fileNames = listFilesFromDir(dir);
		if (fileNames!=null) {
		for (String archiveFile : fileNames){
			if(archiveFile.equals(fileName)) {

				return true;
			}
		}
		}	
		return false;
	}

	/**
	 * Function that opens the Server Socket of the peer. It listens for request and
	 * starts a PeerServerHandler thread every time it gets one. However, if that request is a 
	 * query hit, then it just invokes the queryHit method.
	 * 
	 * @param portNum - Listening Port of the peer.
	 */
	private void startClientServer(int portNum) {
		try {
			ServerSocket server = new ServerSocket(portNum);

			// Listen for a TCP connection request.
			Socket connection = server.accept();

			BufferedReader in = null;
			String method = "";
			String s = "";
			if (dir1==null) {
				 dir1 = "public";
			}
			File f1 = new File(dir1);

			File f2 = new File(dir2);

			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			s = in.readLine();
			System.out.println("S: "+s );
			method = s.split("%")[0];
//out.println("queryhit%" + messageID + "%15%" + fileName + "%" + result.get(i));

			if (method.equals("queryhit")) {
				queryHit(s);
			} else if (method.equals("invalidate")) {
				String fileName = s.split("%")[3];
				if(fileExists(f1, fileName)) {
					boolean deleted = deleteFile(f1,fileName);
					if(deleted) {
						register(portNum);
						System.out.println("File " + fileName + "deleted.");
					} else {
						System.out.println("The file " + fileName + "could not be deleted.");
					}
				} else {
					System.out.println("File does not exist");
				}
			} else {
				// Create a new thread to process the request.

				//String fileName = s.split("%")[4];
				String fileName = s;
				System.out.println("fileName: "+fileName);

				String d = "";
				if((f1!=null)&&(fileExists(f1,fileName))) {

	
						d = dir1;
					
				} 
				else if(fileExists(f2 ,fileName)==true) {
					d = dir2;
				}

				Thread thread = new PeerServerHandler(connection, d, s);

				// Start the thread
				thread.start();
				System.out.println("Thread started for " + portNum);
			}

			server.close();
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}
}
