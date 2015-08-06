package life4food.backend.etl.cov;

import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import life4food.backend.datasource.MySQLDataSource;
import life4food.backend.util.RemoteUrl;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


/**
 * 
 * 產品覆歷
 * @author abola
 *
 *
 * {@link http://data.coa.gov.tw/Query/ServiceDetail.aspx?id=037 農產品交易行情 }
 */
public class ResumeDataPlus {
	
	Type listType = new TypeToken<ArrayList<HashMap<String, Object>>>(){}.getType();
    

	
	String syncDay, start, end ;
	Integer top = 500;
	Integer skip = 0;
	
	// refresh data range: 10 days
	Boolean stopFlag = false;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	Calendar cal ;

	public ResumeDataPlus(){
		cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_YEAR, -10);
	}	
	
	public ResumeDataPlus analyze(){
				
		try{
			String url = "http://data.coa.gov.tw/Service/OpenData/Resume/ResumeData_Plus.aspx?";
			url += "$top=" + top;
			url += "&$skip=" + skip;
			
			System.out.println("call url: " + url);
			String result = RemoteUrl.get(url);
			
			
			List<Map<String, Object>> transformmedResult = new Gson().fromJson(result, listType);
			
			Integer fetchSize = transformmedResult.size();
			System.out.println("current fetch size: " + fetchSize);
			List<String> insertSql = transToInsertSQL(transformmedResult);
			
			MySQLDataSource.execute(insertSql, MySQLDataSource.connectionStringCov);
			
			if( top.equals(fetchSize) &&  false == stopFlag){
				skip += fetchSize;
				return analyze();
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return this;
	}
	
	public List<String> transToInsertSQL(List<Map<String, Object>> results){
		
		String insertSQLResumeFormat = "insert into resume_data("
				+ "trace_code,pack_date,product_name,producer"
				+ ") values('%s','%s','%s','%s') "
				+ "ON DUPLICATE KEY UPDATE "
				+ "product_name=VALUES(product_name)"
				+ ", producer=(producer)"
				;
		String insertSQLHypermarketsFormat = "insert into resume_data_hypermarkets("
				+ "trace_code,pack_date,company"
				+ ") values('%s','%s',%s) "
				+ "ON DUPLICATE KEY UPDATE trace_code = trace_code  " // do nothing
				;		
		
		/*
		 *  { 
		 *  "ProductName":"胡蘿蔔-Carrot"
		 *  ,"OrgID":"132083"
		 *  ,"Producer":"保證責任雲林縣東勢果菜生產合作社"
		 *  ,"Place":"彰化縣芳苑鄉崙腳段"
		 *  ,"FarmerName":"蔡宗志"
		 *  ,"PackDate":"2015/08/06"
		 *  ,"CertificationName":"環球國際驗證股份有限公司"
		 *  ,"ValidDate":"2015/12/08"
		 *  ,"StoreInfo":"■實體通路：愛買量販全國分店、Costco好市多(新竹分店、台中分店、中和分店、汐止分店)"
		 *  ,"OperationDetail":"http://data.coa.gov.tw:80/Service/OpenData/Resume/OperationDetail_Plus.aspx?Tracecode=1040806040300018"
		 *  ,"ResumeDetail":"http://data.coa.gov.tw:80/Service/OpenData/Resume/ResumeDetail_Plus.aspx?Tracecode=1040806040300018","ProcessDetail":"http://data.coa.gov.tw:80/Service/OpenData/Resume/ProcessDetail_Plus.aspx?Tracecode=1040806040300018","CertificateDetail":"http://data.coa.gov.tw:80/Service/OpenData/Resume/CertificateDetail_Plus.aspx?Tracecode=1040806040300018"},
		 */
		List<String> insertSQL = Lists.newArrayList();
		
		for(Map<String, Object> result : results){
			
			String pack_date = result.get("PackDate").toString().replaceAll("\\/", "-");
			String product_name = result.get("ProductName").toString();
			String producer = result.get("Producer").toString();
			String trace_code = result.get("ResumeDetail").toString().substring( "http://data.coa.gov.tw:80/Service/OpenData/Resume/ResumeDetail_Plus.aspx?Tracecode=".length() );
			
			

//			System.out.println(trace_code);
//			System.out.println(pack_date);
//			System.out.println(product_name);
			try{
//				System.out.println(sdf.parse(pack_date));
//				System.out.println(cal.getTime());
				
				// exit when pack date early than 10 days
				if ( sdf.parse(pack_date).before( cal.getTime() ) ){
					stopFlag = true;
					break;
				}
			} catch(Exception e){}
			
			
			List<String>  pendingSQL = Lists.newArrayList();

			String storeInfo = result.get("StoreInfo").toString();
			if ( storeInfo.indexOf("好市多") > -1 ){
				pendingSQL.add(String.format(insertSQLHypermarketsFormat,trace_code,pack_date,1));
			}
			if ( storeInfo.indexOf("家樂福") > -1){
				pendingSQL.add(String.format(insertSQLHypermarketsFormat,trace_code,pack_date,2));
			}
			if ( storeInfo.indexOf("愛買")  > -1){
				pendingSQL.add(String.format(insertSQLHypermarketsFormat,trace_code,pack_date,3));
			}
			if ( storeInfo.indexOf("大潤發")  > -1){
				pendingSQL.add(String.format(insertSQLHypermarketsFormat,trace_code,pack_date,4));
			}
			if ( storeInfo.indexOf("頂好")  > -1){
				pendingSQL.add(String.format(insertSQLHypermarketsFormat,trace_code,pack_date,5));
			}
			if ( storeInfo.indexOf("全聯")  > -1){
				pendingSQL.add(String.format(insertSQLHypermarketsFormat,trace_code,pack_date,6));
			}
			if ( storeInfo.indexOf("松青")  > -1){
				pendingSQL.add(String.format(insertSQLHypermarketsFormat,trace_code,pack_date,7));
			}
				
			// if any Support market in StoreInfo 
			if ( pendingSQL.size() > 0 ){
				
				insertSQL.add(
						String.format(insertSQLResumeFormat
							,trace_code,pack_date,product_name,producer
							)
					);
				insertSQL.addAll(pendingSQL);
			}
		}
		
		return insertSQL;
	} 
	
	public static ResumeDataPlus create(){
		return new ResumeDataPlus();
	}

	public static void main(String[] args) {
		
		ResumeDataPlus.create().analyze();
		
	}
}
