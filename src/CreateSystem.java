import java.io.*;
import java.util.*;

/**
 * @author jcerecedameca@hawk.iit.edu - A20432616
 * @author mtorresgomez@hawk.iit.edu - A20432664
 */

public class CreateSystem {

	/**
	 * Main method of the class CreateSystem. This creates the whole test system, with
	 * the elements that it reads from the config file. Then it serves as a recipient to 
	 * receive data from the command terminal and execute 2 main function, query and retrieve.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		File file = new File("config.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String st;
		int val;
		val = Integer.parseInt(br.readLine());
		if (val == 0) {
			ArrayList<PeerPush> leafs = new ArrayList<PeerPush>();
			while ((st = br.readLine()) != null) {
				
				int serverPort = Integer.parseInt((st.split(":")[1]).split("-")[0]);
				String[] neighbors = (st.split("-")[3]).split(",");
				ArrayList<Integer> superIds = new ArrayList<Integer>();
				for (int i = 0; i < neighbors.length; i++) {
					superIds.add(Integer.parseInt(neighbors[i]));
				}
				ServerPush s = new ServerPush(Integer.parseInt((st.split(":")[1]).split("-")[0]), superIds);

				// thread.start();
				System.out.println("Server " + serverPort + " is running");
	
				String[] nodes = (st.split("-")[1]).split(",");
				String[] dirs = (st.split("-")[2]).split(",");
	
				for (int i = 0; i < nodes.length; i++) {
					PeerPush p = new PeerPush(null,dirs[i], Integer.parseInt(nodes[i]));

					p.setServerPort(serverPort);
					p.register(serverPort);
					leafs.add(p);
					System.out.println("Peer " + nodes[i] + " registered");
				}
			}
			br.close();
	
			System.out.println("");
			System.out.print("Enter command:");
			while (true) {
				BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
	
				try {
					String sg = b.readLine();
					String[] sl = sg.split(";");
					String[] f = sl[0].split(" ");
					if(sl.length >= 1) {
						for (int i = 0; i<sl.length; i++) {
							String[] f1 = sl[i].split(" ");
							if (f1.length == 3 && f1[1].equals("query")) {
								int peerId = Integer.parseInt(f1[0]);
								String fileName = f1[2];
								for (PeerPush p : leafs) {
									
									if (p.getPort() == peerId) {
										System.out.println("query "+i+ " 0time:"+System.currentTimeMillis());
										p.requestFile(fileName);
										System.out.println("Peer " + peerId + " has requested the file");
									}
								}
							} else if (f1.length == 4 && f1[1].equals("retrieve")) {
								int peerId = Integer.parseInt(f1[0]);
								String fileName = f1[3];
								int peerId2 = Integer.parseInt(f1[2]);
								for (PeerPush p : leafs) {
									if (p.getPort() == peerId) {
										System.out.println("retrieve "+i+ " 0time:"+System.currentTimeMillis());
										p.retrieve(fileName, peerId2);
										System.out.println("retrieve "+i+" 1time:"+System.currentTimeMillis());
										System.out.print("Enter command:");
									}
								}	
							} else if(f1.length == 3 && f1[1].equals("update")) {
								int peerId = Integer.parseInt(f1[0]);
								String fileName = f1[2];
								for (PeerPush p : leafs) {
									if (p.getPort() == peerId) {
										//System.out.println("query "+i+ " 0time:"+System.currentTimeMillis());
										p.modifyFile(fileName);
										//System.out.println("Peer " + peerId + " has requested the file");
									}
								}
							}
						} 
					} else {
						System.out.println("Wrong command. Write 'PeerID query fileName' to ask for a file");
					}
				} catch (Exception e) {
					System.err.println(e);
				}
			}
		} else if(val == 1) {
			ArrayList<PeerPull1> leafs = new ArrayList<PeerPull1>();
			while ((st = br.readLine()) != null) {
				
				int serverPort = Integer.parseInt((st.split(":")[1]).split("-")[0]);
				String[] neighbors = (st.split("-")[3]).split(",");
				ArrayList<Integer> superIds = new ArrayList<Integer>();
				for (int i = 0; i < neighbors.length; i++) {
					superIds.add(Integer.parseInt(neighbors[i]));
				}
				Server s = new Server(Integer.parseInt((st.split(":")[1]).split("-")[0]), superIds);
		
				// thread.start();
				System.out.println("Server " + serverPort + " is running");
	
				String[] nodes = (st.split("-")[1]).split(",");
				String[] dirs = (st.split("-")[2]).split(",");
	
				for (int i = 0; i < nodes.length; i++) {
					PeerPull1 p = new PeerPull1(dirs[i], Integer.parseInt(nodes[i]));
					p.setServerPort(serverPort);
					p.register(serverPort);
					leafs.add(p);
					System.out.println("Peer " + nodes[i] + " registered");
				}
			}
			br.close();
	
			System.out.println("");
			System.out.print("Enter command:");
			while (true) {
				BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
	
				try {
					String sg = b.readLine();
					String[] sl = sg.split(";");
					String[] f = sl[0].split(" ");
					if(sl.length >= 1) {
						for (int i = 0; i<sl.length; i++) {
							String[] f1 = sl[i].split(" ");
							if (f1.length == 3 && f1[1].equals("query")) {
								int peerId = Integer.parseInt(f1[0]);
								String fileName = f1[2];
								for (PeerPull1 p : leafs) {
									
									if (p.getPort() == peerId) {
										System.out.println("query "+i+ " 0time:"+System.currentTimeMillis());
										p.requestFile(fileName);
										System.out.println("Peer " + peerId + " has requested the file");
									}
								}
							} else if(f1.length == 3 && f1[1].equals("update")) {
								int peerId = Integer.parseInt(f1[0]);
								String fileName = f1[2];
								for (PeerPull1 p : leafs) {
									if (p.getPort() == peerId) {
										//System.out.println("query "+i+ " 0time:"+System.currentTimeMillis());
										p.update(fileName);
										//System.out.println("Peer " + peerId + " has requested the file");
									}
								}
							} 
						} 
					} else {
						System.out.println("Wrong command. Write 'PeerID query fileName' to ask for a file");
					}
				} catch (Exception e) {
					System.err.println(e);
				}
			}
		
		
		} else if(val == 2) {
			ArrayList<PeerPull2> leafs = new ArrayList<PeerPull2>();
			while ((st = br.readLine()) != null) {
				
				int serverPort = Integer.parseInt((st.split(":")[1]).split("-")[0]);
				String[] neighbors = (st.split("-")[3]).split(",");
				ArrayList<Integer> superIds = new ArrayList<Integer>();
				for (int i = 0; i < neighbors.length; i++) {
					superIds.add(Integer.parseInt(neighbors[i]));
				}
				ServerPull2 s = new ServerPull2(Integer.parseInt((st.split(":")[1]).split("-")[0]), superIds);
		
				// thread.start();
				System.out.println("Server " + serverPort + " is running");
	
				String[] nodes = (st.split("-")[1]).split(",");
				String[] dirs = (st.split("-")[2]).split(",");
	
				for (int i = 0; i < nodes.length; i++) {
					PeerPull2 p = new PeerPull2(dirs[i], Integer.parseInt(nodes[i]), serverPort);
					p.setServerPort(serverPort);
					p.register(serverPort);
					leafs.add(p);
					System.out.println("Peer " + nodes[i] + " registered");
				}
			}
			br.close();
	
			System.out.println("");
			System.out.print("Enter command:");
			while (true) {
				BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
	
				try {
					String sg = b.readLine();
					String[] sl = sg.split(";");
					String[] f = sl[0].split(" ");
					if(sl.length >= 1) {
						for (int i = 0; i<sl.length; i++) {
							String[] f1 = sl[i].split(" ");
							if (f1.length == 3 && f1[1].equals("query")) {
								int peerId = Integer.parseInt(f1[0]);
								String fileName = f1[2];
								for (PeerPull2 p : leafs) {
									if (p.getPort() == peerId) {
										System.out.println("query "+i+ " 0time:"+System.currentTimeMillis());
										p.requestFile(fileName);
										System.out.println("Peer " + peerId + " has requested the file");
									}
								}
							} else if(f1.length == 3 && f1[1].equals("update")) {
								int peerId = Integer.parseInt(f1[0]);
								String fileName = f1[2];
								for (PeerPull2 p : leafs) {
									if (p.getPort() == peerId) {
										//System.out.println("query "+i+ " 0time:"+System.currentTimeMillis());
										p.update(fileName);
										//System.out.println("Peer " + peerId + " has requested the file");
									}
								}
							}
						} 
					} else {
						System.out.println("Wrong command. Write 'PeerID query fileName' to ask for a file");
					}
				} catch (Exception e) {
					System.err.println(e);
				}
			}
	}
	}
}
