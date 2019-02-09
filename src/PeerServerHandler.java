import java.net.*;
import java.io.*;

/**
 * @author jcerecedameca@hawk.iit.edu - A20432616
 * @author mtorresgomez@hawk.iit.edu - A20432664
 * 
 *         PeerServerHandler Class. It is in charge of handling the request that
 *         the Peers receive.
 */
class PeerServerHandler extends Thread {
	private Socket socket = new Socket();
	private String dir = "";
	private String s = "";

	/**
	 * Constructor of the class. It creates a handler and receives the socket to
	 * send the file and the Peer directory in which the file is located.
	 * 
	 * @param socket
	 * @param dir
	 */
	public PeerServerHandler(Socket socket, String dir, String s) {
		super("PeerServerHandler");
		this.socket = socket;
		this.dir = dir;
		this.s = s;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#run()
	 * 
	 * Creates a Buffer Stream to send the file and sends it.
	 */
	@Override
	public void run() {
		try {
			//String file = s.split("%")[3];
			String file = s;
			//String path = f.getAbsolutePath();
			final File reqFile = new File(dir + "/" + file);
			System.out.println("The file " +file + " is going to be sent.");
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(reqFile));

			BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());

			byte[] byteArray = new byte[(int) reqFile.length()];

			int i;
			System.out.println("Sending " + reqFile.getName() + "(" + byteArray.length + " bytes)");
			while ((i = bis.read(byteArray)) != -1) {
				bos.write(byteArray, 0, i);
			}
			System.out.println("Done.");
			//System.out.println("Enter command:");
			bis.close();
			bos.close();
		} catch (Exception e) {
			System.err.println(e + " port " + this.socket.getLocalPort());
		}
	}
}
