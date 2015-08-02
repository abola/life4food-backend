package life4food.backend.etl.cov;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import life4food.backend.util.RemoteUrl;

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
public class FramTransData {
	
	Type listType = new TypeToken<ArrayList<HashMap<String, Object>>>(){}.getType();
    
	public FramTransData(){}
	
	public FramTransData analyze(){
		try{
			String result = RemoteUrl.get("http://m.coa.gov.tw/OpenData/FarmTransData.aspx?$top=10&StartDate=104.07.01&EndDate=104.07.01&Market=" + URLEncoder.encode("三重市", "UTF-8") );
			result = dataClean(result);
			
			
			List<Map<String, Object>> transformmedResult = new Gson().fromJson(result, listType);
			
			System.out.println(transformmedResult.size());
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return this;
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
							 .replaceAll("下價", "bottom_value")
							 .replaceAll("平均價", "mean_value")
							 .replaceAll("交易量", "volumn")
							 ;
		return after;
		
	}
	
	
	public static FramTransData create(){
		return new FramTransData();
	}

	public static void main(String[] args) throws UnsupportedEncodingException {
        System.out.println("Hello! World!");
		FramTransData
			.create()
			.analyze()
			;
		
	}
}
