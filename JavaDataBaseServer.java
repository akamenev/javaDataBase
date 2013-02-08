import java.io.*;
import java.net.*;
import java.util.StringTokenizer;

class JavaDataBaseServer extends Thread {
	Socket serverSocket;
	static DataBase db;
	
	public static void main(String[] args) {
		db = new DataBase("db.json");
		
		try {
			ServerSocket server = new ServerSocket(8082);
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
					if (db.create(key, value)) {
						response = "Record: " + key + " " + value + " was added to database"; 
					} else {
						response = "Record with key:" + key + " already exists";
					}
					break;
				case "read":
					if (db.read(key) != null) {
						response = db.read(key);
					} else {
						response = "Record with key: " + key + " was not found";
					}
					break;
				case "update":
					if (db.update(key, value)) {
						response = "Record with key: " + key + " was updated";
					} else {
						response = "Record with key: " + key + " was not found";
					}
					break;
				case "delete":
					if (db.delete(key)) {
						response = "Record with key: " + key + " was deleted";
					} else {
						response = "Record with key: " + key + " was not found";
					}
					break;
				case "flush":
					db.flush();
					response = "Database was saved";
					break;
				case "size":
					response = db.size() + " records in database";
					break;
			}
			
			
			System.out.println(method + "\n" + key + "\n" + value);
			
			
			os.write(response.getBytes());
			
			serverSocket.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}