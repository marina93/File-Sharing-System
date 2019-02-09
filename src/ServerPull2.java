
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
public class ServerPull2 extends Thread {

	private static final int MAX_ENTRIES = 150;
	private int port;
	ArrayList<Integer> peerIds = new ArrayList<Integer>();
	ArrayList<Integer> superIds = new ArrayList<Integer>();
	HashMap<Integer, ArrayList<String>> peers = new HashMap<Integer, ArrayList<String>>();
	HashMap<Integer, HashMap<String, Long>> peerTTR = new HashMap<Integer, HashMap<String, Long>>();
	HashMap<Integer, HashMap<String, Integer>> peerVersion = new HashMap<Integer, HashMap<String, Integer>>();
	HashMap<Integer, HashMap<String, Integer>> peerOrigin = new HashMap<Integer, HashMap<String, Integer>>();
	HashMap<Integer, HashMap<String, Long>> peerTime = new HashMap<Integer, HashMap<String, Long>>();
	LinkedHashMap<Integer, Integer> messages;
	protected Object lock1 = new Object();

	/**
	 * Constructor of the class. It will create a server socket bound to the
	 * specified port, a Thread to execute the server side of the SuperPeer and 
	 * another thread to execute its consistency method.
	 *
	 * @param port Port in which the ServerSocket of the Server is going to be
	 *             listening.
	 */
	public ServerPull2(int port, ArrayList<Integer> superIds) throws IOException {
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
					checkTTR();
				}
			}
		}).start();
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
			PrintWriter out = new PrintWriter(connection.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String s = in.readLine();
			String method = s.split("%")[0];
			System.out.println("Method: " + method);
			if (method.equals("registry")) {
				System.out.println("Input registry " + s.split("%")[1]);
				String p = s.split("%")[1];
				int peerId = Integer.parseInt(p);
				String dir = s.split("%")[2];
				String sps = "";
				String vs = "";
				String ttrs = "";
				String ts = "";
				String files = "";
				if (s.split("%").length > 3) {
					sps = s.split("%")[3];
					vs = s.split("%")[4];
					ttrs = s.split("%")[5];
					ts = s.split("%")[6];
					files = s.split("%")[7];
				}
				int result = registry(peerId, dir, sps, vs, ttrs, ts, files);
				if (result != 0) {
					DataOutputStream ot = new DataOutputStream(connection.getOutputStream());
					ot.writeUTF("Peer registered correctly");
				} else {
					DataOutputStream ot = new DataOutputStream(connection.getOutputStream());
					ot.writeUTF("Error registering peer");
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
						
						Thread thread = new SuperPeerHandlerPull2(0, result, messages, superIds, port, s, peerOrigin);
						thread.start();
					} else {
						System.out.println("TTL count is 0, message dropped");
					}
				} else {

				}
				server.close();
			} else if (method.equals("queryhit")) {
				ArrayList<Integer> result = new ArrayList<Integer>();
				Thread thread = new SuperPeerHandlerPull2(1, result, messages, superIds, port, s,peerOrigin);
				thread.start();
			} else if (method.equals("update")) {
				String fileName = s.split("%")[1];
				int peer = Integer.parseInt(s.split("%")[2]);
				int version = Integer.parseInt(s.split("%")[3]);
				peerVersion.get(peer).replace(fileName, version);
			} else if (method.equals("poll")) {
				String fileName = s.split("%")[1];
				int version = Integer.parseInt(s.split("%")[2]);
				for (Map.Entry<Integer, ArrayList<String>> entry : peers.entrySet()) {
					if (entry.getValue().contains(fileName)) {
						synchronized (lock1) {
							if (version != peerVersion.get(entry.getKey()).get(fileName)) {
								out.println("invalid%");
								out.close();
							} else {
								out.println(peerTTR.get(entry.getKey()).get(fileName) + "%");
								out.close();
							}
						}
						break;
					}
				}
			}

			// Start the thread.
			System.out.println("Thread started for " + port);

			server.close();
		} catch (Exception e) {
			e.printStackTrace();
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
	 * @param peerId - Identifier of the peer who wants to register to the indexing
	 *                server. This identifier is equals to its port.
	 * @param dirName - Name of the directory of the peer.
	 * @param sps - Superpeers related to the files on the peer directory
	 * @param vs - Versions related to the files on the peer directory
	 * @param ttrs - TTRs related to the files on the peer directory
	 * @param ts - Times related to the files on the peer directory
	 * @param fils - Files of the directory
	 * 
	 * @return int The number of registered peers, or 0 if the peer could not be
	 *         registered.
	 * 
	 */
	public int registry(Integer peerId, String dirName, String sps, String vs, String ttrs, String ts, String fils) 
	{
		String[] sPs = sps.split(",");
		String[] verS = vs.split(",");
		String[] ttrS = ttrs.split(",");
		String[] tS = ts.split(",");
		String[] filS = fils.split(",");
		String files = "";
		final File dir = new File(dirName);
		ArrayList<String> fileNames = new ArrayList<String>();
		fileNames = listFilesFromDir(dir);
		for (int i = 0; i < fileNames.size(); i++) {
			files = fileNames.get(i) + ",";
			System.out.println(files);
		}
		if (peers.containsKey(peerId)) {
			synchronized (lock1) {
				peers.replace(peerId, fileNames);
				if (!sps.equals("")) {
					HashMap<String, Integer> origSP = new HashMap<String, Integer>();
					HashMap<String, Integer> vers = new HashMap<String, Integer>();
					HashMap<String, Long> ttr = new HashMap<String, Long>();
					HashMap<String, Long> time = new HashMap<String, Long>();
					for (int i = 0; i < filS.length; i++) {
						origSP.put(filS[i], Integer.parseInt(sPs[i]));
						vers.put(filS[i], Integer.parseInt(verS[i]));
						ttr.put(filS[i], Long.parseLong(ttrS[i]));
						time.put(filS[i], Long.parseLong(tS[i]));
					}
					peerOrigin.replace(peerId, origSP);
					peerVersion.replace(peerId, vers);
					peerTime.replace(peerId, time);
					peerTTR.replace(peerId, ttr);
				}

				return peers.size();
			}

		} else if (!peers.containsKey(peerId)) {
			synchronized (lock1) {
				if (!sps.equals("")) {
					HashMap<String, Integer> origSP = new HashMap<String, Integer>();
					HashMap<String, Integer> vers = new HashMap<String, Integer>();
					HashMap<String, Long> ttr = new HashMap<String, Long>();
					HashMap<String, Long> time = new HashMap<String, Long>();
					for (int i = 0; i < filS.length; i++) {
						origSP.put(filS[i], Integer.parseInt(sPs[i]));
						vers.put(filS[i], Integer.parseInt(verS[i]));
						ttr.put(filS[i], Long.parseLong(ttrS[i]));
						time.put(filS[i], Long.parseLong(tS[i]));
					}
					peerIds.add(peerId);
					peers.put(peerId, fileNames);
					peerOrigin.put(peerId, origSP);
					peerVersion.put(peerId, vers);
					peerTime.put(peerId, time);
					peerTTR.put(peerId, ttr);
				}
				return peers.size();

			}

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

	/**
	 * Method to poll a file. It sends a poll message to the SuperPeer that controls the peer that owns
	 * the file and receives its answer. If it is an invalid, it removes the file from the peer's directory. 
	 * Otherwise, it will update the value of the TTR and the value of time of the file.
	 * 
	 * @param file - file to be polled
	 * @param origin - SuperPeer origin of the file
	 * @param version - Current version of the file
	 * @param peer - Peer that owns the file
	 */
	public void pollFile(String file, int origin, int version, int peer) {
		try {
			Socket socket = new Socket("localhost", origin);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out.println("poll%"+file+"%"+version);
			String r = in.readLine();
			String m = r.split("%")[0];
			in.close();
			out.close();
			socket.close();
			synchronized (lock1) {
				if (m.equals("invalid")) {
					Socket leaf = new Socket("localhost", peer);
					PrintWriter outLeaf = new PrintWriter(leaf.getOutputStream(), true);
					peerOrigin.get(peer).remove(file);
					peerVersion.get(peer).remove(file);
					peerTTR.get(peer).remove(file);
					peerTime.get(peer).remove(file);
					peers.get(peer).remove(file);
					outLeaf.println("invalid%" + file);
					outLeaf.close();
					leaf.close();
				} else {
					peerTTR.get(peer).replace(file, Long.parseLong(m));
					peerTime.get(peer).replace(file, System.currentTimeMillis());
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}

	}

	
	/**
	 * Method that iterates the files of the peer and checks the time it has been without updating.
	 * If TTR is less than that value, it calls the poll method on the correspondent file.
	 */
	public void checkTTR() {
		try {
			synchronized (lock1) {
				for (Map.Entry<Integer, HashMap<String, Long>> entry : peerTime.entrySet()) {
					for (Map.Entry<String, Long> file : entry.getValue().entrySet()) {
						if (peerOrigin.get(entry.getKey()).get(file.getKey()) != this.port) {
							if ((System.currentTimeMillis() - entry.getValue().get(file.getKey())) >= peerTTR.get(entry.getKey()).get(file.getKey())) {
								pollFile(file.getKey(), (peerOrigin.get(entry.getKey())).get(file.getKey()),
										peerVersion.get(entry.getKey()).get(file.getKey()), entry.getKey());
							}
						}
					}
				}
			}
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}
}
