package xiaolong.baidu.db;

import java.sql.*;
import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;

public class BaiduIndexDB {
	private Connection conn = null;
	//private String dburl= "jdbc:mysql://localhost/test?user=root&password=cxl";

	public BaiduIndexDB() throws ClassNotFoundException, SQLException{
			this("localhost","tracker","root", "cxl");
	}
	
	public BaiduIndexDB(String dbhost, String dbname, String dbuser, String dbpass) throws ClassNotFoundException, SQLException{
		String dburl= "jdbc:mysql://"+dbhost+"/"+dbname+"?useUnicode=true&characterEncoding=UTF8&characterSetResults=UTF8&user="+dbuser+"&password="+dbpass;
		Class.forName("com.mysql.jdbc.Driver");
		this.conn= DriverManager.getConnection(dburl);	
	}
	
	/*
	 * This function inserts fetched data into database
	 * NOTICE: The 3 input arrays/lists can be different in length
	 */
	public void insertData(String keyword, ArrayList<DateTime> dates, String[] userIndexesSplit, String[] mediaIndexesSplit) 
			throws SQLException{
	
		DateTime last= DateTime.parse("2000-01-01");
		
		//get last updated date
		Statement stmt= this.conn.createStatement();
		ResultSet rs= stmt.executeQuery("select MAX(date) from baidu_index_flash where keyword=\""+keyword+"\"");
		if( rs.next() ){
			if(rs.getString(1)!=null){
				//System.out.println(rs.getString(1));
				last= DateTime.parse(rs.getString(1));			
			}
		}
		
		int num_days= dates.size();
		int num_user= userIndexesSplit.length;
		int num_media= mediaIndexesSplit.length;
		
		int min= (num_days< num_user)?num_days:num_user;
		min= (min<num_media)?min:num_media;
		
		for (int i=min;i>0; i--){
			if( dates.get(num_days-i).isAfter(last)){
				LocalDate date= dates.get(num_days-i).toLocalDate();
				int userCount=0;
				int mediaCount=0;
				if(userIndexesSplit[num_user-i].length()>0) userCount=Integer.parseInt(userIndexesSplit[num_user-i]);
				if(mediaIndexesSplit[num_media-i].length()>0)  mediaCount=Integer.parseInt(mediaIndexesSplit[num_media-i]);
				if(userCount==0 && mediaCount==0) continue;//skip empty data

				stmt.execute("insert into baidu_index_flash (date, keyword, userVolume, mediaVolume) values (\""+date+"\",\""+keyword+"\", "+userCount+", "+mediaCount+") ");
				System.out.println(userIndexesSplit[num_user-i]+" #  "+mediaIndexesSplit[num_media-i]+"  #  "+dates.get(num_days-i));			
			}
		}
		System.out.println("Data for ["+keyword+"] inserted!");
	}
	
	/*
	 * get the keyword list that needs to be crawled
	 */
	public ArrayList<String> getKeywordsList(){
		ArrayList<String> words= new ArrayList<String>();
		try{
			Statement stmt=this.conn.createStatement();
			ResultSet rs= stmt.executeQuery("select keyword from baidu_keywords_template where keywordSetId=1 AND id>1405");	
			while(rs.next()){
				words.add(rs.getString(1));
				System.out.println(rs.getString(1));
			}
		} catch(SQLException e){} 
		
		return words;
	}


	public static void main(String[] args) throws ClassNotFoundException, SQLException{

	}

}