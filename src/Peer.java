import java.net.*;
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
public class Peer {

	private static final int TTL = 15;
	private int messID;
	private int port;
	private String dir;
	private int serverPort;

	/**
	 * Constructor of the class. It will identify a single Peer and start its Server
	 * side.
	 * 
	 * @param dir  Directory in which the Peers files are located.
	 * @param port Port in which the ServerSocket of the Peer is going to be
	 *             listening.
	 */
	public Peer(String dir, int port) {
		this.dir = dir;
		this.port = port;
		this.messID = 0;
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
			out.println("registry%" + this.port + "%" + this.dir);
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
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dir + "/" + fileName));
			int i;
			byte[] receivedData = new byte[6022386];
			while ((i = bis.read(receivedData)) != -1) {
				bos.write(receivedData, 0, i);
			}
			bos.close();
			out.close();
			
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

		//System.out.println("Filename " + fileName + " found in: " + sPort);
		//System.out.println("query "+this.port+ " time:"+System.currentTimeMillis());
		//System.out.print("Enter command: ");
		try {
			retrieve(fileName, Integer.parseInt(sPort));
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
			String method = "";
			String s = "";

			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			s = in.readLine();
			method = s.split("%")[0];

			if (method.equals("queryhit")) {
				//System.out.println("Entro queryHit peer " + s);
				queryHit(s);
			} else {
				// Create a new thread to process the request.
				System.out.println("Dir2 " + this.dir);
				Thread thread = new PeerServerHandler(connection, this.dir, s);

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
