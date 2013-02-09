import java.io.*;
import java.net.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Balancer extends Thread{
	Socket serverSocket;
	static String router;
	static int shardNumber = 0;
	static ArrayList<String> masters;
	static ArrayList<String> slaves01;
	static ArrayList<String> slaves02;
	
	public static void main(String[] args) {
		String routerConfig = "config.json";
		masters = new ArrayList<String>();
		slaves01 = new ArrayList<String>();
		slaves02 = new ArrayList<String>();
		
		File checkFile = new File(routerConfig);
		
		if (checkFile.exists()) {
			JSONParser parser = new JSONParser();
			try {
				Object obj = parser.parse(new FileReader(routerConfig));
				JSONObject jsObj = (JSONObject) obj;
				router = jsObj.get("router").toString();
				JSONArray jsArray = (JSONArray)parser.parse(jsObj.get("shards").toString());
				
				for (Object o : jsArray) {
					JSONObject jsonObject = (JSONObject) o;
					
					masters.add(jsonObject.get("master").toString());
					slaves01.add(jsonObject.get("slave01").toString());
					slaves02.add(jsonObject.get("slave02").toString());	
					shardNumber++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		
		try {
			StringTokenizer parseRouter = new StringTokenizer(router, ":");
			parseRouter.nextToken();
			ServerSocket server = new ServerSocket(Integer.parseInt(parseRouter.nextToken()));
			System.out.println("JavaDataBase balancer started");
			
			while (true) {
				new Balancer(server.accept());
			}	
			
		} catch (Exception e) {
			e.printStackTrace();
		}
			
			
	}
	
	public Balancer(Socket serverSocket) {
		this.serverSocket = serverSocket;
		setDaemon(true);
		setPriority(NORM_PRIORITY);
		start();
	}
		
	public void run() {
		try {
			InputStream is = serverSocket.getInputStream();
			OutputStream os = serverSocket.getOutputStream();
			byte buf[] = new byte[64*1024];
			int r = is.read(buf);
			String data = new String(buf, 0, r);
				
			String response = "";
			String inputRequest = data;
			data = data.substring(0, data.indexOf("\r"));
			data = data.replace("HTTP/1.1", "");
			String method = "";
			String key = "";
				
			StringTokenizer getParser = new StringTokenizer(data, " =&/");
			if (data.contains("GET") && data.contains("method")) {
				int tokenNumbers = getParser.countTokens();
				for (int i = 0; i < tokenNumbers; i++) {
					String word = getParser.nextToken();
					if (i == 2) {
						method = word;
					}
					if (i == 4) {
						key = word;
					}
				}
			}
			
			
			
			if (method.equals("create") || method.equals("update") || method.equals("delete")) {
				if (key != "") {
					int shard = Math.abs(key.hashCode()%shardNumber);
					StringTokenizer master = new StringTokenizer (masters.get(shard), ":");
					Socket masterSocket = new Socket(master.nextToken(), Integer.parseInt(master.nextToken()));
					masterSocket.getOutputStream().write(inputRequest.getBytes());
					InputStream iMaster = masterSocket.getInputStream();
					byte bufMaster[] = new byte[64*1024];
					int rMaster = iMaster.read(bufMaster);
					response = new String (bufMaster, 0, rMaster);
				} 
			} else if (method.equals("flush")) {
				for (int i = 0; i < shardNumber; i ++) {
					StringTokenizer master = new StringTokenizer (masters.get(i), ":");
					Socket masterSocket = new Socket(master.nextToken(), Integer.parseInt(master.nextToken()));
					masterSocket.getOutputStream().write(inputRequest.getBytes());
					InputStream iMaster = masterSocket.getInputStream();
					byte bufMaster[] = new byte[64*1024];
					int rMaster = iMaster.read(bufMaster);
					response = new String (bufMaster, 0, rMaster);
				}
			} else if (method.equals("read")) {
				if (key != "") {
					int shard = Math.abs(key.hashCode()%shardNumber);
					
					if (shard%3 == 0) {
						StringTokenizer master = new StringTokenizer (masters.get(shard), ":");
						Socket masterSocket = new Socket(master.nextToken(), Integer.parseInt(master.nextToken()));
						masterSocket.getOutputStream().write(inputRequest.getBytes());
						InputStream iMaster = masterSocket.getInputStream();
						byte bufMaster[] = new byte[64*1024];
						int rMaster = iMaster.read(bufMaster);
						response = new String (bufMaster, 0 , rMaster);
					} else if (shard%3 == 1) {
						StringTokenizer slave01 = new StringTokenizer (slaves01.get(shard), ":");
						Socket slave01Socket = new Socket(slave01.nextToken(), Integer.parseInt(slave01.nextToken()));
						slave01Socket.getOutputStream().write(inputRequest.getBytes());
						InputStream iSlave01 = slave01Socket.getInputStream();
						byte bufSlave01[] = new byte[64*1024];
						int rSlave01 = iSlave01.read(bufSlave01);
						response = new String (bufSlave01, 0 , rSlave01);
					} else {
						StringTokenizer slave02 = new StringTokenizer (slaves02.get(shard), ":");
						Socket slave02Socket = new Socket(slave02.nextToken(), Integer.parseInt(slave02.nextToken()));
						slave02Socket.getOutputStream().write(inputRequest.getBytes());
						InputStream iSlave02 = slave02Socket.getInputStream();
						byte bufSlave02[] = new byte[64*1024];
						int rSlave02 = iSlave02.read(bufSlave02);
						response = new String (bufSlave02, 0 , rSlave02);
					}		
					
					
				}
			} else if (method.equals("size")) {
				int records = 0;
				for (int i = 0; i < shardNumber; i ++) {
					StringTokenizer master = new StringTokenizer (masters.get(i), ":");
					Socket masterSocket = new Socket(master.nextToken(), Integer.parseInt(master.nextToken()));
					masterSocket.getOutputStream().write(inputRequest.getBytes());
					InputStream iMaster = masterSocket.getInputStream();
					byte bufMaster[] = new byte[64*1024];
					int rMaster = iMaster.read(bufMaster);
					
					StringTokenizer record = new StringTokenizer (new String (bufMaster, 0, rMaster), " ");
					records = records + Integer.parseInt(record.nextToken());
				}
				response = records + " records in database";	

			}
			
			
			os.write(response.getBytes());
			serverSocket.close();		
				
		} catch (Exception e) {
			e.printStackTrace();
		}
			
	}			
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}