package life4food.backend.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

public class RemoteUrl {

	static int ALWAYS_URL_ENCODE = 1;
	
	/**
	 *  取得遠端url資訊
	 * @return
	 */
	public static String get(String url){
	    InputStream is;
	    InputStreamReader isr;
	    BufferedReader r;
	    String str;

	    
	    StringBuilder sb = new StringBuilder();
	    
	    try {
		    

	      is = new URL(url).openStream();		
	      isr = new InputStreamReader(is);
	      r = new BufferedReader(isr);
	      do {
	        str = r.readLine();
	        if (str != null)
	          sb.append(str);
	      } while (str != null);
	    } catch (MalformedURLException e) {
	      System.out.println("Must enter a valid URL");
	      return "";
	    } catch (IOException e) {
	      return "";
	    }
		return sb.toString();
	}
}
