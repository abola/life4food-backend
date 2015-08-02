package life4food.backend.etl.cov;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import life4food.backend.datasource.MySQLDataSource;
import life4food.backend.util.RemoteUrl;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


/**
 * 
 * 農產品交易行情
 * @author abola
 *
 *
 * {@link http://data.coa.gov.tw/Query/ServiceDetail.aspx?id=037 農產品交易行情 }
 */
public class FarmTransData {
	
	Type listType = new TypeToken<ArrayList<HashMap<String, Object>>>(){}.getType();
    
	public FarmTransData(){
		
	}
	public FarmTransData(String day){
		this.syncDay = Ints.tryParse(day.substring(0,4))-1911 
						+ "." + day.substring(5,7) 
						+ "." + day.substring(8,10);
		
		System.out.println("process day: "+ syncDay);
		
	}
	
	String syncDay ;
	Integer top = 500;
	Integer skip = 0;
	
	
	public FarmTransData analyze(){
		try{
			String url = "http://m.coa.gov.tw/OpenData/FarmTransData.aspx?";
			url += "$top=" + top;
			url += "&$skip=" + skip;
			if ( null != syncDay ){
				url += "&StartDate="+syncDay+"&EndDate=" + syncDay;
			}
			// &Market=" + URLEncoder.encode("三重市", "UTF-8")
			System.out.println("call url: " + url);
			String result = RemoteUrl.get(url);
			
			result = dataClean(result);
			
			
			List<Map<String, Object>> transformmedResult = new Gson().fromJson(result, listType);
			
			Integer fetchSize = transformmedResult.size();
			System.out.println("current fetch size: " + fetchSize);
			List<String> insertSql = transToInsertSQL(transformmedResult);
			
			MySQLDataSource.execute(insertSql, MySQLDataSource.connectionStringCov);
			
			if( top.equals(fetchSize) ){
				skip += fetchSize;
				return analyze();
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return this;
	}
	
	public List<String> transToInsertSQL(List<Map<String, Object>> results){
		
		String insertSQLFormat = "insert into farm_trans_data("
				+ "trade_day,crop_code,crop_name,market_code,market_name"
				+ ",top_value,medium_value,low_value,mean_value,volumn"
				+ ") values('%s','%s','%s',%s,'%s',%s,%s,%s,%s,%s) "
				+ "ON DUPLICATE KEY UPDATE "
				+ "top_value=VALUES(top_value)"
				+ ", medium_value=(medium_value)"
				+ ", low_value=(low_value)"
				+ ", mean_value=(mean_value)"
				+ ", volumn=(volumn)"
				;
		
		List<String> insertSQL = Lists.newArrayList();
		
		for(Map<String, Object> result : results){
			String unformatedDay = result.get("trade_day").toString();
			String trade_day = Ints.tryParse(unformatedDay.substring(0,3))+1911 
								+ "-" + unformatedDay.substring(4,6)  
								+ "-" + unformatedDay.substring(7,9) ;

			String crop_code = result.get("crop_code").toString();
			String crop_name = result.get("crop_name").toString();
			String market_code = result.get("market_code").toString();
			String market_name = result.get("market_name").toString();
			String top_value = result.get("top_value").toString();
			String medium_value = result.get("medium_value").toString();
			String low_value = result.get("low_value").toString();
			String mean_value = result.get("mean_value").toString();
			String volumn = result.get("volumn").toString();
			
			insertSQL.add(
				String.format(insertSQLFormat
					,trade_day,crop_code,crop_name,market_code,market_name
					,top_value,medium_value,low_value,mean_value,volumn
					)
			);
			
		}
		
		return insertSQL;
	} 
	
	/**
	 * 資料清理，將中文字的key 轉換
	 * @param before
	 * @return
	 */
	public String dataClean(String before){
		String after = before.replaceAll("交易日期", "trade_day")
							 .replaceAll("作物代號", "crop_code")
							 .replaceAll("作物名稱", "crop_name")
							 .replaceAll("市場代號", "market_code")
							 .replaceAll("市場名稱", "market_name")
							 .replaceAll("上價", "top_value")
							 .replaceAll("中價", "medium_value")
							 .replaceAll("下價", "low_value")
							 .replaceAll("平均價", "mean_value")
							 .replaceAll("交易量", "volumn")
							 ;
		return after;
		
	}
	

	public static FarmTransData create(){
		return new FarmTransData();
	}
	public static FarmTransData create(String day){
		return new FarmTransData(day);
	}

	public static void main(String[] args) throws UnsupportedEncodingException {
		
		if ( 0 < args.length ){
			FarmTransData.create(args[0]).analyze();
		}else{
			FarmTransData.create().analyze();
		}
		
	}
}
