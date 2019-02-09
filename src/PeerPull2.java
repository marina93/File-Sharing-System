import java.net.*;
import java.util.*;
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
public class PeerPull2{

	private static final int TTL = 15;
	private int messID;
	private int port;
	private String dir;
	private int serverPort;
	HashMap<String, Integer> fileVersion = new HashMap<String, Integer>();
	HashMap<String, Integer> fileOrigin = new HashMap<String, Integer>();
	HashMap<String, Integer> fileSP = new HashMap<String, Integer>();
	HashMap<String, Long> fileTime = new HashMap<String, Long>();
	HashMap<String, Long> fileTTR = new HashMap<String, Long>();
	protected Object lock1 = new Object();

	/**
	 * Constructor of the class. It will identify a single Peer and start its Server
	 * side.
	 * 
	 * @param dir  Directory in which the Peers files are located.
	 * @param port Port in which the ServerSocket of the Peer is going to be
	 *             listening.
	 */
	public PeerPull2(String dir, int port, int serverPort) {
		this.dir = dir;
		this.port = port;
		this.messID = 0;
		this.serverPort = serverPort;
		final File dirN = new File(dir);
		synchronized (lock1) {
			for (final File archiveFile : dirN.listFiles()) {
				fileVersion.put(archiveFile.getName(), 1);
				fileOrigin.put(archiveFile.getName(), this.port);
				fileSP.put(archiveFile.getName(), this.serverPort);
				fileTime.put(archiveFile.getName(), System.currentTimeMillis());
				fileTTR.put(archiveFile.getName(), (long)60000);
			}
		}
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
	public String getDir() {
		return this.dir;
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
	
	/**
	 * Method to update a file. It increases its version value in one.
	 * 
	 * @param fileName - File to be updated
	 */
	public void update(String fileName) {
		synchronized (lock1) {
			fileVersion.replace(fileName, fileVersion.get(fileName)+1);
		}
		PrintWriter out = null;
		Socket socket = null;
		try {
			socket = new Socket("localhost", this.serverPort);
			out = new PrintWriter(socket.getOutputStream(), true);
			synchronized (lock1) {
				out.println("update%"+fileName+"%"+this.port+"%"+(fileVersion.get(fileName)));
			}
			System.out.println("File "+fileName+ "updated to version "+fileVersion.get(fileName));
			System.out.print("Enter command: ");
		} catch (Exception e) {
			System.out.println(e);
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
		String sps = "";
		String vs = "";
		String ttrs = "";
		String ts = "";
		String files = "";
		synchronized (lock1) {
			for(Map.Entry<String, Integer> entry : fileSP.entrySet()) {
				files += entry.getKey()+ ",";
				sps += entry.getValue().toString() + ","; 
			}
			for(Map.Entry<String, Integer> entry : fileVersion.entrySet()) {
				vs += entry.getValue().toString()+ ","; 
			}
			for(Map.Entry<String, Long> entry : fileTTR.entrySet()) {
				ttrs += entry.getValue().toString()+ ","; 
			}
			for(Map.Entry<String, Long> entry : fileTime.entrySet()) {
				ts += entry.getValue().toString()+ ","; 
			}
		}
		try {
			socket = new Socket("localhost", portNumber);
			out = new PrintWriter(socket.getOutputStream(), true);
			out.println("registry%" + this.port + "%" + this.dir + "%" +sps +"%" +vs +"%"+ttrs+"%"+ts+"%"+files);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			System.out.println("Message Received: " + in.readLine());
			System.out.print("Enter command:");
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
	public void retrieve(String fileName, int port, int spOrigin) throws Exception {
		try {
			Socket socket = new Socket("localhost", port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			out.println(fileName);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String s = in.readLine();
			int version = Integer.parseInt(s.split("%")[0]);
			long ttr = Long.parseLong(s.split("%")[1]);
			int origin = Integer.parseInt(s.split("%")[2]);
			BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dir + "/" + fileName));
			int i;
			byte[] receivedData = new byte[6022386];
			while ((i = bis.read(receivedData)) != -1) {
				bos.write(receivedData, 0, i);
			}
			System.out.println("File " + fileName + " downloaded");
			synchronized (lock1) {
				if (fileVersion.containsKey(fileName)) {
					fileOrigin.replace(fileName, origin);
					fileTime.replace(fileName, System.currentTimeMillis());
					fileVersion.replace(fileName, version);
					fileTTR.replace(fileName, ttr);
					fileSP.replace(fileName, spOrigin);
				} else {
					fileOrigin.put(fileName, origin);
					fileTime.put(fileName, System.currentTimeMillis());
					fileVersion.put(fileName, version);
					fileTTR.put(fileName, ttr);
					fileSP.put(fileName, spOrigin);
				}
			}
			register(serverPort);
			bos.close();
			out.close();
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
			out.println("query%" + this.port + this.messID + "%" + TTL + "%" + fileName + "%" + this.port);
			this.messID +=1;
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
	private void queryHit(String s) {
		String fileName = s.split("%")[3];
		String sPort = s.split("%")[4];
		String spOrig = s.split("%")[5];

		try {
			if(!fileOrigin.containsKey(fileName)) {
				System.out.println("File found on SP "+spOrig+" and leaf "+sPort);
				retrieve(fileName, Integer.parseInt(sPort), Integer.parseInt(spOrig));
			}	
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
			PrintWriter out = null;
			String method = "";
			String s = "";

			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			s = in.readLine();
			method = s.split("%")[0];

			if (method.equals("queryhit")) {
				queryHit(s);
			} else if(method.equals("invalid")) {
				String file = s.split("%")[1];
				synchronized (lock1){
					int origin = fileOrigin.get(file);
					int sp = fileSP.get(file);
					fileTTR.remove(file);
					fileOrigin.remove(file);
					fileTime.remove(file);
					fileVersion.remove(file);
					fileSP.remove(file);
					System.out.println("File "+file+" outdated. You can replace it by retrieving the file from "+origin+ "and "+sp);
					File f = new File(this.dir+"/"+file);
					f.delete();
					register(serverPort);
				}
				
			} else {
				// Create a new thread to process the request.
				synchronized (lock1) {
					Thread thread = new PeerPullServerHandler2(connection, this.dir, s, fileVersion.get(s), fileTTR.get(s), fileOrigin.get(s));

					// Start the thread
					thread.start();
				}		
				System.out.println("Thread started for " + portNum);
			}

			server.close();
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}
}
