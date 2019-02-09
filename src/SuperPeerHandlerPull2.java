
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.io.*;

class SuperPeerHandlerPull2 extends Thread {
	private int method;
	ArrayList<Integer> result = new ArrayList<Integer>();
	LinkedHashMap<Integer, Integer> messages = new LinkedHashMap<Integer, Integer>();
	ArrayList<Integer> superIds = new ArrayList<Integer>();
	private int port;
	String s;
	HashMap<Integer, HashMap<String, Integer>> peerOrigin = new HashMap<Integer, HashMap<String, Integer>>();

	public SuperPeerHandlerPull2(int method, ArrayList<Integer> result, LinkedHashMap<Integer, Integer> messages,
			ArrayList<Integer> superIds, int port, String s, HashMap<Integer, HashMap<String, Integer>> peerOrigin) {
		super("SuperPeerHandler");
		this.method = method;
		this.result = result;
		this.messages = messages;
		this.superIds = superIds;
		this.port = port;
		this.s = s;
		this.peerOrigin = peerOrigin;
	}

	/**
	 * Run function. This function is in charge of handling the requests from the
	 * peers. Allows creating a connection with peers, analyzes their requests and
	 * executes the corresponding method (query or queryhit).
	 */

	@Override
	public void run() {
		if (method == 0) {
			int messageID = Integer.parseInt(s.split("%")[1]);
			int ttl = Integer.parseInt(s.split("%")[2]) - 1;
			int prevID = Integer.parseInt(s.split("%")[4]);
			String fileName = s.split("%")[3];
			if (!result.isEmpty()) {
				try {
					for (int i = 0; i < result.size(); i++) {
						Socket socket = new Socket("localhost", prevID);
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						int portSP = peerOrigin.get(result.get(i)).get(fileName);
						out.println("queryhit%" + messageID + "%15%" + fileName + "%" + result.get(i)+"%"+portSP);
						socket.close();
					}
				} catch (Exception e) {
					System.err.println(e + " port " + this.port);
				}
			} else {
				try {
					for (int i = 0; i < superIds.size(); i++) {
						if (superIds.get(i) != prevID) {
							Socket socket = new Socket("localhost", superIds.get(i));
							PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
							out.println("query%" + messageID + "%" + ttl + "%" + fileName + "%" + this.port);
							socket.close();
						}
					}
				} catch (Exception e) {
					System.err.println(e + " port " + this.port);
				}
			}
		} else if (method == 1) {
			int messageID = Integer.parseInt(s.split("%")[1]);
			int ttl = Integer.parseInt(s.split("%")[2]) - 1;
			int leafID = Integer.parseInt(s.split("%")[4]);
			int portOrigin = Integer.parseInt(s.split("%")[5]);
			String fileName = s.split("%")[3];
			queryhit(messageID, ttl, fileName, leafID, portOrigin);
		}
	}

	/**
	 * Method to backpropagate the query hit message to the query original sender. 
	 * 
	 * @param msgID - ID of the received message
	 * @param TTL - Time to live of the message
	 * @param fileName - Name of the requested file
	 * @param port - port of the leaf node who host the file
	 */
	public void queryhit(int msgID, int TTL, String fileName, int port, int portOrigin) {
		PrintWriter out = null;
		int spPort = messages.get(msgID);
		try {
			TTL--;
			Socket socket = new Socket("localhost", spPort);
			out = new PrintWriter(socket.getOutputStream(), true);
			out.println("queryhit%" + msgID + "%" + TTL + "%" + fileName + "%" + port +"%"+portOrigin);
			socket.close();
		} catch (Exception e) {
			System.err.println(e + " port " + this.port);
		}
	}

}
