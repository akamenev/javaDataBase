interface CRUD {
	
	public Boolean create(String key, String value);
	public String read(String key);
	public Boolean update(String key, String value);
	public Boolean delete(String key);	

	
}