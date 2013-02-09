import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.StringTokenizer;

class JavaDataBaseClient {
	public static void main(String[] args) {
		System.out.println("JavaDataBase client started\n" +
							"Available commands: create, read, update, delete, flush, size\n" +
							"Type exit to exit\n");
		String ip = args[0];
		String port = args[1];
		String cmd = "";
	
		while(!cmd.equals("exit")) {
			System.out.print("jdb > ");
			Scanner input = new Scanner(System.in);
			cmd = input.nextLine();
			//System.out.println(cmd);
			
			StringTokenizer cmdParser = new StringTokenizer(cmd, "(,)");
			if (cmd.contains("size") || cmd.contains("flush")) {
				cmd = "method=" + cmd;
				System.out.println(writeRequest(ip, port, cmd));
			}else if (cmd.contains("read") || cmd.contains("delete")) {
				cmd = "method=" + cmdParser.nextToken() + "&key=" + cmdParser.nextToken();
				System.out.println(writeRequest(ip, port, cmd));
			} else if (cmd.contains("create") || cmd.contains("update")) {
				cmd = "method=" + cmdParser.nextToken() + "&key=" + cmdParser.nextToken() + "&value=";
				String tmp = cmdParser.nextToken();
				if (tmp.charAt(0) == ' ') {
					tmp = tmp.substring(1, tmp.length());
				}
				
				cmd = cmd + tmp;
				cmd = cmd.replace(" ", "%20");
				System.out.println(writeRequest(ip, port, cmd));
			} else {
				System.out.println("Illegal command");
			}
		}	
	}
	
	
	private static String writeRequest(String ip, String port, String cmd) {
			String data = "";
		try {
			String request = "GET /" + cmd + " HTTP/1.1\r\n" +
							 "Host: " + ip + ":" + port + "\r\n" +
							 "User-Agent: JavaDataBase client\r\n";
			Socket socket = new Socket(ip, Integer.parseInt(port));
			socket.getOutputStream().write(request.getBytes());
			
			byte buf[] = new byte[64*1024];
			int r = socket.getInputStream().read(buf);
			data = new String(buf, 0, r);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}
}