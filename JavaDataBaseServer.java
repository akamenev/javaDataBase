import java.io.*;
import java.net.*;
import java.util.StringTokenizer;
import java.util.ArrayList;

class JavaDataBaseServer extends Thread {
	Socket serverSocket;
	static DataBase db;
	static ArrayList<String> slavePorts;
	static ArrayList<String> slaveAddr;
	static int slaveNumber;
	
	public static void main(String[] args) {
		db = new DataBase("db.json");
		slavePorts = new ArrayList<String>();
		slaveAddr = new ArrayList<String>();
		Boolean isMaster = false;
		slaveNumber = 0;
		
		if ((args.length > 1) && (args.length % 2 == 0)) {
			if (args[1].equals("-m")) {
				slaveNumber = args.length/2 - 1;
				for (int i = 2; i < args.length; i++) {
					if (i%2 == 0) {
						slaveAddr.add(args[i]);
					} else {
						slavePorts.add(args[i]);
					}
				}
			}
		}
		
		//System.out.println(slavePorts.get(0));
			
		try {
			ServerSocket server = new ServerSocket(Integer.parseInt(args[0]));
			System.out.println("JavaDataBase server is started");
			
			// listening port
			while (true) {
				// waiting for new connection
				new JavaDataBaseServer(server.accept());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public JavaDataBaseServer(Socket serverSocket) {
		this.serverSocket = serverSocket;
		setDaemon(true);
		setPriority(NORM_PRIORITY);
		start();
	}
	
	public void run() {
		try {
			// from client socket get input data stream
			InputStream is = serverSocket.getInputStream();
			// get output data stream
			OutputStream os = serverSocket.getOutputStream();
			// data buffer 64KB
			byte buf[] = new byte[64*1024];
			int r = is.read(buf);
			// String which contatins client data
			String data = new String(buf, 0, r);
			// response to client
			String response = "";
			
			System.out.println("\n" + data + "\n");
			String inputRequest = data;
			data = data.substring(0, data.indexOf("\r"));
			data = data.replace("HTTP/1.1", "");
			String method = "";
			String key = "";
			String value = "";
			
			
			StringTokenizer getParser = new StringTokenizer(data, " =&/");
			if (data.contains("GET") && data.contains("method")) {
				int tokenNumber = getParser.countTokens();
				for (int i = 0; i < tokenNumber; i++) {
					String word = getParser.nextToken();
					if (i == 2) {
						method = word; 
					}
					if (i == 4) {
						key = word;	
					}
					if (i == 6) {
						value = word;
					}
					
				}
			}
			value = value.replace("%20", " ");
			
			switch (method) {
				case "create":
					if ((key != "") && (value != "")) {
						if (db.create(key, value)) {
							response = "Record: " + key + " " + value + " was added to database";
							for (int i = 0; i < slaveNumber; i ++) {
								Socket slaveSocket = new Socket(slaveAddr.get(i), Integer.parseInt(slavePorts.get(i)));
								slaveSocket.getOutputStream().write(inputRequest.getBytes());
							}
						} else {
							response = "Record with key:" + key + " already exists";
						}
					} else {
						response = "Illegal argument";
					}
					break;
				case "read":
					if (key != "") {
						if (db.read(key) != null) {
							response = db.read(key);
						} else {
							response = "Record with key: " + key + " was not found";
						}
					} else {
						response = "Illegal argument";
					}
					break;
				case "update":
					if ((key != "") && (value != "")) {
						if (db.update(key, value)) {
							response = "Record with key: " + key + " was updated";
							for (int i = 0; i < slaveNumber; i ++) {
								Socket slaveSocket = new Socket(slaveAddr.get(i), Integer.parseInt(slavePorts.get(i)));
								slaveSocket.getOutputStream().write(inputRequest.getBytes());
							}
						} else {
							response = "Record with key: " + key + " was not found";
						}
					} else {
						response = "Illegal argument";
					}
					break;
				case "delete":
					if (key != "") {
						if (db.delete(key)) {
							response = "Record with key: " + key + " was deleted";
							for (int i = 0; i < slaveNumber; i ++) {
								Socket slaveSocket = new Socket(slaveAddr.get(i), Integer.parseInt(slavePorts.get(i)));
								slaveSocket.getOutputStream().write(inputRequest.getBytes());
							}
						} else {
							response = "Record with key: " + key + " was not found";
						}
					} else {
						response = "Illegal argument";
					}
					break;
				case "flush":
					db.flush();
					response = "Database was saved";
					for (int i = 0; i < slaveNumber; i ++) {
								Socket slaveSocket = new Socket(slaveAddr.get(i), Integer.parseInt(slavePorts.get(i)));
								slaveSocket.getOutputStream().write(inputRequest.getBytes());
							}	
					break;
				case "size":
					response = db.size() + " records in database";
					break;
			}
			
			
			//System.out.println(method + "\n" + key + "\n" + value);
			
			
			os.write(response.getBytes());
			
			serverSocket.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}