package xiaolong.baidu.run;

import java.io.IOException;
import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
//import java.util.Map;

import java.security.NoSuchAlgorithmException;

import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import xiaolong.baidu.libs.BaiduIndexEncoder;
import xiaolong.baidu.db.*;

import flex.messaging.io.amf.client.AMFConnection;
import flex.messaging.io.amf.client.exceptions.ClientStatusException;
import flex.messaging.io.amf.client.exceptions.ServerStatusException;
import flex.messaging.messages.AcknowledgeMessage;
import flex.messaging.messages.RemotingMessage;


public class BaiduIndexFetcher {
	
	private String gateway_url= "http://index.baidu.com/gateway.php";
	
	//private String key=null;
	private String keyStr=null;
	private String areaStr=null;
	private String periodsStr=null;
	private String timeStr=null;
	private String hashStr=null;
	
	private List<DateTime> dates=null;
	private String[] mediaIndexesSplit=null;
	private String[] userIndexesSplit=null;
	
	
	public BaiduIndexFetcher(String keysStr, String areasStr, String periodsStr, String timeStr, String hashStr){
		this.keyStr= keysStr;
		this.areaStr=areasStr;
		this.periodsStr= periodsStr;
		this.timeStr= timeStr;
		this.hashStr= hashStr;
	}
	
	public void fetch() throws ClientStatusException, ServerStatusException, ClassNotFoundException, SQLException{
		AMFConnection amfConnection = new AMFConnection();

		try {
			amfConnection.connect(this.gateway_url);
			amfConnection.setObjectEncoding(3);

			amfConnection.addHttpRequestHeader("Content-type", "application/x-amf");
			amfConnection.addHttpRequestHeader("Referer", "http://index.baidu.com/fla/TrendAnalyserfc9e1047.swf");

			RemotingMessage msg = new RemotingMessage();
			msg.setOperation("DataAccessor.getIndexes");
			msg.setBody(new Object[] { this.keyStr, this.areaStr, this.periodsStr, this.timeStr, this.hashStr });

			AcknowledgeMessage reply = (AcknowledgeMessage) amfConnection.call("null", msg);
			//System.out.println(reply);
			Object body = reply.getBody();
			//System.out.println(body);
			
/*		
			System.out.println(body);
			Object[] arr = (Object[]) body;
			Map map = (Map) arr[0];
			String indexes = (String) map.get("userIndexes");
			String baiduIndex = BaiduIndexEncoder.getBaiduIndex("1", indexes);
			System.out.println(baiduIndex);
*/	 
			
			
			String bodyStr= body.toString();
			if(bodyStr.length()>0) this.processMessage(bodyStr);
			
		} finally {
			amfConnection.close();
		}
	}
	
	public void processMessage(String bodyStr) throws ClassNotFoundException, SQLException{
		Pattern p = Pattern.compile("(area|Name|userIndexes|mediaIndexes|period|key)++");
		String[] split = p.split( bodyStr );//split the message body string	
		
		String area= split[1].substring(1,split[1].length()-2);
		String areaName=split[2].substring(1,split[2].length()-2);
		
		String userIndexes= split[3].substring(1,split[3].length()-2);
		userIndexes= new BaiduIndexEncoder().getBaiduIndex("1",userIndexes);//decoding
		//System.out.println(userIndexes);
		String[] userIndexesSplit = userIndexes.split(",");

		
		String mediaIndexes= split[4].substring(1,split[4].length()-2);
		String[] mediaIndexesSplit= mediaIndexes.split(",");
		
		String period= split[5].substring(1,split[5].length()-2);
		//System.out.println(period);
		String[] dateRange= period.split("\\|");
		
		
		DateTimeFormatter formatter= DateTimeFormat.forPattern("yyyy-MM-dd");
		DateTime startDate= formatter.parseDateTime(dateRange[0]);
		DateTime endDate= formatter.parseDateTime(dateRange[1]);
		
		String key= split[6].substring(1,split[6].length()-2);
		
		ArrayList<DateTime> dates = new ArrayList<DateTime>();
		int days_gap = Days.daysBetween(startDate, endDate).getDays();
		
		for (int i=0; i <= days_gap; i++) {
		    dates.add(startDate.plusDays(i));
		}
		
		System.out.println(userIndexesSplit.length+" "+dates.size()+" "+mediaIndexesSplit.length);
		
		/* Now we have all 3 lists: dates, userIndexes, mediaIndexes		
		* NOTICE: these 3 lists can be different in length in some situation 
		* special cases handled in DB class
		*/
		this.dates= dates;
		this.userIndexesSplit= userIndexesSplit;
		this.mediaIndexesSplit= mediaIndexesSplit;		

		// dump data to DB
		new BaiduIndexDB(Config.HOST,Config.DATABASE,Config.USER,Config.PASS).insertData(this.keyStr, dates, userIndexesSplit, mediaIndexesSplit);
	
	}
	
	public List<DateTime> getDates(){
		return this.dates;
	}
	public String[] getUserIndexes(){
		return this.userIndexesSplit;
	}
	public String[] getMediaIndexes(){
		return this.mediaIndexesSplit;
	}


	public static void main(String[] args) throws ClientStatusException, ServerStatusException, IOException, ClassNotFoundException, SQLException, NoSuchAlgorithmException {

		BaiduIndexFetcher f= new BaiduIndexFetcher("仙侠世界", "0", "", "c1e56860094945d160", "bba181873929c9b98329a4cfeac5837b");	
		f.fetch();
	}

}