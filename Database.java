import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Scanner;

import org.w3c.dom.ranges.Range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.*; //get files

public class Database {
	private Connection connect()
	{
		Connection conn = null;
        Scanner getDBNameObj = new Scanner(System.in);
        try {
        	System.out.println("Enter the name of the database");
        	String DBName = getDBNameObj.nextLine();
            // db parameters
            String url = "jdbc:sqlite:/home/lavickas/Downloads/"+DBName+".db";
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            System.out.println("Connection to SQLite has been established.");
        } 
        catch (SQLException e) 
        {
            System.out.println(e.getMessage());
        } 
        return conn;
	}
	
	public List<List<String>> readCSV(String file){
		file = "./"+file+".csv";
		String delimiter = ",";
		String line;
		List<List<String>> lines = new ArrayList();
			try (BufferedReader br =
						new BufferedReader(new FileReader(file))) {
				while((line = br.readLine()) != null){
					List<String> values = Arrays.asList(line.split(delimiter));
					lines.add(values);
				}
				//lines.forEach(l -> System.out.println(l));
			} catch (Exception e){
				System.out.println(e);
			}
		return lines;



	}

	
	public void insertforPlayer(Integer accountNum, String charName, String ForeName, String surname, String email ) {
        String sql = "INSERT INTO Player(Account_Number,Character_Name,Forename,surname,e-mail_address) VALUES(?,?,?,?,?)";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, accountNum);
            pstmt.setString(2, charName);
			pstmt.setString(3, ForeName);
			pstmt.setString(4,surname);
			pstmt.setString(5, email);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

	public void Insert(String file){
		
		//Connection conn = null;

		List<List<String>> lines = new ArrayList();
		Connection conn = this.connect();
		if(file == "Players"){
			lines = readCSV("Customers");
			lines.remove(0);
			for(int i=0;i<lines.size();i++){
				//values to extract are 0,6,1,2,3
				List<String> line = lines.get(i);
				//insertforPlayer(Integer.parseInt(line.get(0)), line.get(6), line.get(1), line.get(2), line.get(3));
				String sql = "INSERT OR IGNORE INTO Players(AccountNumber,Forename,Surname,EmailAddress) VALUES(?,?,?,?)";
	
				
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					int accountNum = parser(line.get(0) );
					pstmt.setInt(1, accountNum);
					pstmt.setString(2, line.get(1));
					pstmt.setString(3, line.get(2));
					pstmt.setString(4, line.get(3));

					pstmt.executeUpdate();
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				}
				
			}


		}

		else if(file == "CharacterOwn"){
			lines = readCSV("Customers");
			lines.remove(0);

			for(int i=0;i<lines.size();i++){
				//values to extract are 0,6,1,2,3
				List<String> line = lines.get(i);
				//insertforPlayer(Integer.parseInt(line.get(0)), line.get(6), line.get(1), line.get(2), line.get(3));
				String sql = "INSERT OR IGNORE INTO CharacterOwn(AccountNumber,CharacterName,Health,AttackinScore,DefenceScore,ManaScore,CharacterCreateDate,CharacterExpiryDate) VALUES(?,?,?,?,?,?,?,?)";
				// order for fetch: 6, 10, 15, 4,5
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					
					int AccountNumber=0;
					int Health=0;
					int manaScore=0;
					int AttackinScore = 0;
					int DefenceScore=0;

					AccountNumber = parser(line.get(0));
					Health = parser(line.get(11));
					AttackinScore = parser(line.get(12));
					DefenceScore = parser(line.get(13));
					manaScore = parser(line.get(14));

				
					pstmt.setInt(1,AccountNumber);
					pstmt.setString(2, line.get(6) ); //char name
					pstmt.setInt(3, Health);
					pstmt.setInt(4, AttackinScore);
					pstmt.setInt(5,DefenceScore);
					pstmt.setInt(6,manaScore);
					pstmt.setString(7, line.get(4));
					pstmt.setString(8,line.get(5));

					pstmt.executeUpdate();
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				}
				
			}
		}
		else if(file == "BattlePartakeIn"){

			List<List<String>> linesCust = new ArrayList();
			linesCust= readCSV("Customers");
			lines = readCSV("Combat");
			
			lines.remove(0);
			linesCust.remove(0);
			
			List<String> Names = new ArrayList();
			for(int i=0;i<lines.size();i++){
				List<String> line = lines.get(i);
				if(!Names.contains(line.get(2))){
					Names.add(line.get(2));
				}
			}
			
			for(int j=1;j<Names.size()+1;j++){
				
				String sql = "INSERT OR IGNORE INTO BattlePartakeIn(BattleNo,AccountNumber,CharacterName) VALUES(?,?,?)";
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
						
					int AccountNumber=0;
					String charName=Names.get(j-1); //get attacker name
					for(int x=0 ; j<linesCust.size(); x++ ){
						List<String> lineC = linesCust.get(x);
						
						if(charName.compareTo(lineC.get(6) )==0 ){
							//grab account Number
							AccountNumber = parser(lineC.get(0));
							break;
						}
					}
					
					pstmt.setInt(1,j);
					pstmt.setInt(2,AccountNumber);

					pstmt.setString(3,Names.get(j-1)); 
					
					pstmt.executeUpdate();
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				}
			}

			 
		}
			
		
		else if(file == "Battles"){
			List<List<String>> linesCust = new ArrayList();
			linesCust= readCSV("Customers");
			lines = readCSV("Combat");
			lines.remove(0);
			linesCust.remove(0);
			
			for(int i=0;i<lines.size();i++){
				//values to extract are 0,6,1,2,3
				List<String> line = lines.get(i);
				//insertforPlayer(Integer.parseInt(line.get(0)), line.get(6), line.get(1), line.get(2), line.get(3));
				String sql = "INSERT OR IGNORE INTO Battles(AccountNumber,CharacterName,Attacker,Defender,Weapon,Result,Damage) VALUES(?,?,?,?,?,?,?)"; //not using BattleNo as it is an autoincrement field
				// order for fetch: 6, 10, 15, 4,5
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
					int AccountNumber = 0;
					
					int Damage=parser(line.get(6));
					String charName=line.get(2); //get attacker name
					for(int j=0 ; j<linesCust.size(); j++ ){
						List<String> lineC = linesCust.get(j);
						
						if(charName.compareTo(lineC.get(6) )==0 ){
							//grab account Number
							AccountNumber = parser(lineC.get(0));
							break;
						}
					}
					
					
					//pstmt.setInt(1,    BattleNo );
					pstmt.setInt(1,AccountNumber);
					pstmt.setString(2,charName);
					pstmt.setString(3, line.get(2));
					pstmt.setString(4, line.get(3));
					pstmt.setString(5, line.get(4));
					pstmt.setString(6, line.get(5));
					pstmt.setInt(7,	   Damage);

					pstmt.executeUpdate();
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				}
				
			}
		}
		else if(file == "InventoryItems"){
			List<List<String>> linesCust = new ArrayList();
			linesCust= readCSV("Customers");
			lines = readCSV("Items");
			lines.remove(0);
			linesCust.remove(0);
			
			for(int i=0;i<lines.size();i++){
				//values to extract are 0,6,1,2,3
				List<String> line = lines.get(i);
				//insertforPlayer(Integer.parseInt(line.get(0)), line.get(6), line.get(1), line.get(2), line.get(3));
				String sql = "INSERT OR IGNORE INTO InventoryItems(AccountNumber,CharacterName,Item,ItemType,WeaponType,Range,Quantity,DefenseScore,AttackScore,HealingScore,ManaScore,BodyPart) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"; 
				//System.out.println(line);
				try (PreparedStatement pstmt = conn.prepareStatement(sql)) {

					int AccountNumber = 0;
					int Range=0;
					int Quantity=0;
					int DefenseScore = 0;
					int HealingScore = 0;
					int ManaScore = 0;
					int AttackScore=0;


					String charName=line.get(0); //get attacker name
					for(int j=0 ; j<linesCust.size(); j++ ){
						List<String> lineC = linesCust.get(j);
						
						if(charName.compareTo(lineC.get(6) )==0 ){
							//grab account Number
							AccountNumber = parser(lineC.get(0));
							break;
						}
					}

				
					Quantity = parser(line.get(6));
					Range = parser(line.get(4));
					DefenseScore = parser(line.get(7));
					AttackScore = parser(line.get(8));
					HealingScore = parser(line.get(9));
					ManaScore = parser(line.get(10));
					
					pstmt.setInt(1,AccountNumber);
					pstmt.setString(2, line.get(0)); //character
					pstmt.setString(3, line.get(1)); //item
					pstmt.setString(4, line.get(2) ); //item type
					pstmt.setString(5, line.get(3)); //weapon type
					pstmt.setInt(6,	Range ); // 
					pstmt.setInt(7,Quantity); //quantity
					pstmt.setInt(8,DefenseScore);
					pstmt.setInt(9,AttackScore);
					pstmt.setInt(10,HealingScore);
					pstmt.setInt(11,ManaScore);
					pstmt.setString(12,line.get(14));

					pstmt.executeUpdate();
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				}
				
			}
		}
		
		}

		public int parser(String line){
			int value=0;
			try{
				value = Integer.parseInt(line);
				return value;
			}
			catch(Exception e){
				//
				return value;
			}
			
		}
		
	public void SQLQueries(int type){
		Connection conn = this.connect();
		if(type==1){
			try{
				String sql = "SELECT DISTINCT Attacker,Result FROM Battles WHERE RESULT='Hit' ORDER BY Result DESC LIMIT 5; ";
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql);

				while(rs.next()){
					System.out.println(rs.getString("Attacker"));
					
				}
			}
			catch(SQLException e){
				System.out.println(e.getMessage());
			}
					
		}

	}
	

	public void CreateTable(String TBName) 
	 {
		
		 Connection conn = null;
		// Scanner getDBNameObj = new Scanner(System.in);
	        try {
	        	
	        	conn = this.connect();
	        	//System.out.println("Enter the name of the Table TO CREATE");
	        	//String TBName = getDBNameObj.nextLine();
	        	Statement stmt = conn.createStatement();
				int query = 0;
				//Read in csv file
				if(TBName=="Players"){
					//System.out.println("Access");
					String sql = "CREATE TABLE Players " +
                       "(AccountNumber INT PRIMARY KEY   NOT NULL,"+
                       " Forename            TEXT     NOT NULL, " + 
                       " Surname        TEXT, " + 
                       " EmailAddress         TEXT NOT NULL )";

					//query = stmt.executeUpdate("CREATE TABLE "+TBName+" (Account_Number INT PRIMARY KEY NOT NULL, CHARACTER_NAME TEXT NOT NULL, FORENAME TEXT NOT NULL, SURNAME TEXT, e-mail_address TEXT NOT NULL)");
					stmt.executeUpdate(sql);
					stmt.close(); //always add close statements otherwise DB is corrupted
					conn.close();
				}
				else if(TBName == "CharacterOwn"){
					String sql = "CREATE TABLE CharacterOwn " +
                       "(AccountNumber INT    NOT NULL," +
					   " CharacterName TEXT NOT NULL, "+ 
                       " Health       INT NOT NULL, " +
					   " AttackinScore INT NOT NULL," +
					   " DefenceScore INT NOT NULL," +
					   " ManaScore       INT, " + 
					   " CharacterCreateDate      TEXT NOT NULL, " + 
                       " CharacterExpiryDate         TEXT," +
					   " PRIMARY KEY(AccountNumber,CharacterName) ," +
					   " FOREIGN KEY(AccountNumber) REFERENCES Players(AccountNumber) ON DELETE CASCADE ,"+
					   " FOREIGN KEY(AccountNumber,CharacterName) REFERENCES BattlePartakeIn(AccountNumber,CharacterName) ,"+
					   " FOREIGN KEY(AccountNumber,CharacterName) REFERENCES InventoryItems(AccountNumber,CharacterName) )";
					   
					   stmt.executeUpdate(sql);
					   stmt.close(); //always add close statements otherwise DB is corrupted
					   conn.close();

				}
				else if(TBName == "Battles"){
					String sql = "CREATE TABLE Battles " +
                       "(BattleNo INTEGER PRIMARY KEY AUTOINCREMENT    NOT NULL," +
					    "AccountNumber INT NOT NULL,"+
						"CharacterName TEXT NOT NULL,"+
                       " Attacker            TEXT NOT NULL , " + 
					   " Weapon           TEXT    NOT NULL, " + 
                       " Defender       TEXT NOT NULL, " + 
					   " Damage       INT NOT NULL, " + 
                       " Result         TEXT NOT NULL )";

					   stmt.executeUpdate(sql);
					   stmt.close(); //always add close statements otherwise DB is corrupted
					   conn.close();

				}
				else if(TBName == "BattlePartakeIn"){
					String sql = "CREATE TABLE BattlePartakeIn " +
                       "(BattleNo INTEGER  NOT NULL," +
                       " AccountNumber           INT    NOT NULL, " + 
                       " CharacterName            TEXT NOT NULL , "+
					   " PRIMARY KEY(BattleNo,AccountNumber,CharacterName) ,"+
					   " FOREIGN KEY(AccountNumber,CharacterName) REFERENCES Battles(AccountNumber,CharacterName) )"; 
                      
					   stmt.executeUpdate(sql);
					   stmt.close(); //always add close statements otherwise DB is corrupted
					   conn.close();

				}
				else if(TBName == "InventoryItems"){
					String sql = "CREATE TABLE InventoryItems" +
                       "(AccountNumber INT     NOT NULL," +
					   " CharacterName TEXT NOT NULL," +
                       " Item           TEXT    NOT NULL, " + 
                       " ItemType            TEXT NOT NULL, " + 
                       " WeaponType     TEXT, " + 
					   " Range       INT, " +
                       " Quantity INT NOT NULL, " + 
					   " DefenseScore       INT, " +
					   " AttackScore            INT, " +
					   " HealingScore        INT,"  +
                       " ManaScore     INT, " + 
                       " BodyPart       TEXT,"+
					   " PRIMARY KEY(AccountNumber,CharacterName,Item) )";
					   
					   stmt.executeUpdate(sql);
					   stmt.close(); //always add close statements otherwise DB is corrupted
					   conn.close();

				}
	        	
	        	System.out.println("Table is created");	        	
	        }
	        catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }finally { try {
	        	
	            if (conn != null) {
	                conn.close();
	            }
	        } catch (SQLException ex) {
	            System.out.println(ex.getMessage());
	        	}}
	        
	    }
	 public void PrintTable() 
	 {
		 Connection conn = null;
		 Scanner getDBNameObj = new Scanner(System.in);
	        try {
	        	System.out.println("PrintTable");
	            conn = this.connect();
	            System.out.println("Enter the name of the Table TO PRINT");
	        	String TBName = getDBNameObj.nextLine(); 
	        	Statement UrazsQuery = conn.createStatement();
	        	ResultSet UrazsAnswer = UrazsQuery.executeQuery("SELECT * FROM "+TBName+"; ");
	        	if(UrazsAnswer.next()==false)
	        		System.out.println("Empty Table");
	        	else
	        	{
	        		String stname = UrazsAnswer.getString(1);
	        		int stage =  UrazsAnswer.getInt("STAGE");
	        		String stid = UrazsAnswer.getString(3);
	        		System.out.println(stname+" " + stage + " " + stid);
	        		while(UrazsAnswer.next())
		        	{
		        		stname = UrazsAnswer.getString(1);
		        		stage =  UrazsAnswer.getInt(2);
		        		stid = UrazsAnswer.getString(3);
		        		System.out.println(stname+" " + stage + " " + stid);
		        	}	
	        	}
	        	
	        	
	        }
	        catch (SQLException e) {
	            System.out.println(e.getMessage());
	        } finally { try {
	        	
	            if (conn != null) {
	                conn.close();
	            }
	        } catch (SQLException ex) {
	            System.out.println(ex.getMessage());
	        	}}
	        
	    }
		/**
	     * @param args the command line arguments
	     */
	    public static void main(String[] args) {
	    	Database db = new Database();
	    		
			  
			  db.CreateTable("Players");
			  db.CreateTable("CharacterOwn");
			  db.CreateTable("BattlePartakeIn");


			  db.CreateTable("Battles");
			  db.CreateTable("InventoryItems");

			
			  db.Insert("Players");
			  db.Insert("CharacterOwn");
			  db.Insert("Battles");
			  db.Insert("BattlePartakeIn");
			  db.Insert("InventoryItems");
			  
			  db.SQLQueries(1);
			
			  
	    }
}

/*
OUTPUT:
CreateTable
Enter the name of the database
Uraz
Connection to SQLite has been established.
Enter the name of the Table
SCC203
Table is created
PrintTable
Enter the name of the database
Uraz
Connection to SQLite has been established.
Enter the name of the Table
SCC203
Empty Table
*/
