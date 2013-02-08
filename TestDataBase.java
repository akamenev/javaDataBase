public class TestDataBase {
	public static void main (String[] args) {
		DataBase db = new DataBase("db.json");
		
		//db.create("ivanov", "Иванов Иван Иванович 91344924");
		System.out.println(db.read("ivanov"));
		
		//db.create("kamenev", "Каменев Андрей Владимирович 12141434");
		
		db.delete("kamenev");
		
		db.flush();
		
	}
}