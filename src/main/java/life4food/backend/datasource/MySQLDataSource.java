package life4food.backend.datasource;

import java.util.List;
import java.util.Map;

public class MySQLDataSource extends AbstractDataSource {
	 
	static String driver  = "com.mysql.jdbc.Driver";
	
	public String connectionStringCov = "jdbc:mysql://128.199.204.20/cov?user=cov&password=";
	// default
	public String connectionString = connectionStringCov;
	
	@Override protected String getDriver(){
		return driver;
	}
	
	@Override protected String getConnectionString() {
		return connectionString;
	}

	@Override protected void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}

	
	public static List<Map<String, Object>> executeQuery(String sql) throws DataSourceException{
		return new MySQLDataSource().query(sql);
	}
	
	public static List<Map<String, Object>> executeQuery(String sql, String connectionString ) throws DataSourceException{
		MySQLDataSource mds = new MySQLDataSource();
		mds.setConnectionString( connectionString );
		return mds.query(sql);
	}
	

	
	public static void execute(String sql, String connectionString ) throws DataSourceException{
		MySQLDataSource mds = new MySQLDataSource();
		mds.setConnectionString( connectionString );
		mds.execute(sql);
	}	
}
