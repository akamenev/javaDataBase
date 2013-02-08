import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.File;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class DataBase implements CRUD {
	HashMap<String, String> dataBaseMap = new HashMap<String, String>();
	String dataBaseFile;
	
	public DataBase(String dataBaseFile) {
		this.dataBaseFile = dataBaseFile;
		
		File checkFile = new File(dataBaseFile);
		
		if (checkFile.exists()) {
			if (checkFile.length() != 0) {
				JSONParser parser = new JSONParser();
				
				try {
					JSONArray jsArray = (JSONArray)parser.parse(new FileReader(dataBaseFile));
					
					for (Object o : jsArray) {
						JSONObject jsonObject = (JSONObject) o;
						String key = (String) jsonObject.get("key");
						String value = (String) jsonObject.get("value");
						
						this.dataBaseMap.put(key, value);
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
			}
		} else {
			try {
				checkFile.createNewFile();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	
	}
	
	public long size() {
		return this.dataBaseMap.size();
	}
	
	public Boolean create(String key, String value) {
		if (!dataBaseMap.containsKey(key)) {
			dataBaseMap.put(key, value);
			return true;
		}
		return false;
	}
	
	public String read(String key) {
		if (dataBaseMap.containsKey(key)) {
			return dataBaseMap.get(key).toString();
		}
		return null;
	}
	
	public Boolean update(String key, String value) {
		if (dataBaseMap.containsKey(key)) {
			dataBaseMap.put(key, value);
			return true;
		}
		return false;
	}
	
	public Boolean delete(String key) {
		if (dataBaseMap.containsKey(key)) {
			dataBaseMap.remove(key);
			return true;
		}
		return false;
	}
	
	public void flush() {
		try (FileWriter file = new FileWriter(dataBaseFile)) {
			file.write("[");
			for (Map.Entry<String, String> entry : dataBaseMap.entrySet()) {
				JSONObject obj = new JSONObject();
				obj.put("key", entry.getKey());
				obj.put("value", entry.getValue());
				file.write(obj.toJSONString());
			}
			file.write("]");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}














