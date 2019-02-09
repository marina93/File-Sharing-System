
import java.net.*;
import java.util.*;
import java.io.*;

/**
 * @author jcerecedameca@hawk.iit.edu - A20432616
 * @author mtorresgomez@hawk.iit.edu - A20432664
 *
 *         SuperPeer Class. It contains all the necessary methods to create a
 *         SuperPeer able to communicate correctly with peers, handle and
 *         execute their requests.
 */
public class Server extends Thread {

	private static final int MAX_ENTRIES = 150;
	private int port;
	ArrayList<Integer> peerIds = new ArrayList<Integer>();
	ArrayList<Integer> superIds = new ArrayList<Integer>();
	HashMap<Integer, ArrayList<String>> peers = new HashMap<Integer, ArrayList<String>>();
	LinkedHashMap<Integer, Integer> messages;

	/**
	 * Constructor of the class. It will create a server socket bound to the
	 * specified port, and a Thread to execute the server side of the SuperPeer.
	 *
	 * @param port Port in which the ServerSocket of the Server is going to be
	 *             listening.
	 */
	public Server(int port, ArrayList<Integer> superIds) throws IOException {
		this.port = port;
		this.superIds = superIds;
		messages = new LinkedHashMap<Integer, Integer>() {
			@Override
			protected boolean removeEldestEntry(Map.Entry<Integer, Integer> eldest) {
				return size() > MAX_ENTRIES;
			}
		};
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					startSuperPeer();
				}
			}
		}).start();
	}

	/**
	 * Function that executes the server side of the SuperPeer. It receives the
	 * petitions from the leaf nodes and from other super peers and handles them
	 * adequately, creating a Thread if necessary.
	 * 
	 */
	public void startSuperPeer() {
		try {
			ServerSocket server = new ServerSocket(port);

			// Listen for a TCP connection request.
			Socket connection = server.accept();

			// Create a new thread to process the request.
			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String s = in.readLine();
			String method = s.split("%")[0];
			System.out.println("Method: " + method);
			if (method.equals("registry")) {

				System.out.println("Input registry " + s.split("%")[1]);
				String p = s.split("%")[1];
				int peerId = Integer.parseInt(p);
				String dir = s.split("%")[2];
				int result = registry(peerId, dir);
				if (result != 0) {
					DataOutputStream out = new DataOutputStream(connection.getOutputStream());
					out.writeUTF("Peer registered correctly");
				} else {
					DataOutputStream out = new DataOutputStream(connection.getOutputStream());
					out.writeUTF("Error registering peer");
				}
				connection.close();
			} else if (method.equals("query")) {
				System.out.println("Input query");
				int messageID = Integer.parseInt(s.split("%")[1]);
				int ttl = Integer.parseInt(s.split("%")[2]) - 1;
				int prevID = Integer.parseInt(s.split("%")[4]);
				String fileName = s.split("%")[3];
				if (!messages.containsKey(messageID)) {
					if (ttl != 0) {
						messages.put(messageID, prevID);
						ArrayList<Integer> result = search(fileName);
						Thread thread = new SuperPeerHandler(0, result, messages, superIds, port, s);
						thread.start();
					} else {
						System.out.println("TTL count is 0, message dropped");
					}
				}
				server.close();
			} else if (method.equals("queryhit")) {
				ArrayList<Integer> result = new ArrayList<Integer>();
				Thread thread = new SuperPeerHandler(1, result, messages, superIds, port, s);
				thread.start();
			}

			// Start the thread.
			System.out.println("Thread started for " + port);

			server.close();
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}

	/**
	 * Auxiliary method to list files from a directory
	 *
	 * @param dir Directory whose files are going to be listed.
	 * @return ArrayList<String> containing the files of the directory dir
	 */
	public ArrayList<String> listFilesFromDir(final File dir) {
		ArrayList<String> fileNames = new ArrayList<String>();
		for (final File archiveFile : dir.listFiles()) {
			fileNames.add(archiveFile.getName());
		}
		return fileNames;
	}

	/**
	 * Registry function. This function is in charge of registering peers to the
	 * indexing server. Receives the identifier of the peer and the name of its
	 * directory. It calls the auxiliary function listFilesFromDir(final File dir)
	 * to extract all the files contained in the directory.
	 *
	 * It checks if the peer has already been registered. If it has not been
	 * registered, it registers the peer on the indexing server. If it has been
	 * registered, then the server replaces the previous entrance of the peer with a
	 * new entrance containing all the new files.
	 *
	 *
	 * @param peerId  Identifier of the peer who wants to register to the indexing
	 *                server. This identifier is equals to its port.
	 * @param dirName Name of the directory of the peer.
	 * @return int The number of registered peers, or 0 if the peer could not be
	 *         registered.
	 * 
	 */
	public int registry(Integer peerId, String dirName) {
		String files = "";

		final File dir = new File(dirName);
		ArrayList<String> fileNames = new ArrayList<String>();
		fileNames = listFilesFromDir(dir);
		for (int i = 0; i < fileNames.size(); i++) {

			files = fileNames.get(i) + ",";
			System.out.println(files);
		}

		if (peers.containsKey(peerId)) {

			peers.replace(peerId, fileNames);
			return peers.size();

		} else if (!peers.containsKey(peerId)) {

			peerIds.add(peerId);
			peers.put(peerId, fileNames);
			return peers.size();

		} else {

			return 0;
		}
	}

	/**
	 * Search function. This function is in charge of looking for the peers who
	 * contain a file. It receives the name of the file that a client is searching.
	 * The method checks the files contained in the directories of all peers, and
	 * returns the identifier of those peers who have the file.
	 *
	 *
	 * @param fileName Name of the file that the peer is looking for.
	 * @return public ArrayList<Integer> of the identifiers of the peers who have
	 *         the file.
	 */
	public ArrayList<Integer> search(String fileName) {
		ArrayList<Integer> matchingPeers = new ArrayList<Integer>();
		for (int i = 0; i < peerIds.size(); i++) {
			int peerId = peerIds.get(i);
			ArrayList<String> fileNames = peers.get(peerId);
			for (int j = 0; j < fileNames.size(); j++) {
				if (fileName.equals(fileNames.get(j))) {
					matchingPeers.add(peerId);
				}
			}
		}
		return matchingPeers;
	}
}
