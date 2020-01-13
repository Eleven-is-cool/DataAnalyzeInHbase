
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.util.HashMap;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Data_Analyze{

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    /**
     * @param args
     */
    
  //建立连接
    public static void init(){
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //关闭连接
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void QueryTopMovie(String tableName) {
        
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("imdb_score"), CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes("9.0"));
            filters.add(filter1);
           // Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("Score"), null, CompareOp.EQUAL,
            //        Bytes.toBytes("90"));
          //  filters.add(filter2);

           /* Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
                    Bytes.toBytes("ccc"));
            filters.add(filter3);*/
            FilterList filterList1 = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            //System.out.println(rs);  
            System.out.println("IMDB电影评分大于9.0的电影:");
    	    try 
    	    {
    	         for (Result r : rs) {
    	             for (Cell cell : r.listCells()) {
    	            	 String row = new String(CellUtil.cloneRow(cell));
    	            	 if(!row.equals("1")){//no care number one row
	      	            	 String find = new String(CellUtil.cloneQualifier(cell));
	    	            	 if(find.equals("imdb_score") || find.equals("movie_title") || find.equals("country") )//show title_name
	    	            	 {
		    	                 //System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
		    	                 //System.out.println("Timetamp(时间戳):"+cell.getTimestamp()+" ");
		    	                 //System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
		    	                 //System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
		    	                 //System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
	    	            		 System.out.print(new String(CellUtil.cloneQualifier(cell))+" : ");
		    	                 System.out.println(new String(CellUtil.cloneValue(cell))+" ");
		    	            
	    	            	 }	            		 
    	            	 }
    	             }
    	             System.out.println();
    	         }  	       
    	    }finally {
   	         	close();
        }
        }catch(Exception e){
        	e.printStackTrace();
        }
        
   }
    
    public static int QueryScore(String tableName,int range) {
    	int sum = 0;
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("imdb_score"), CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes(String.valueOf(range)));
            filters.add(filter1);
           // Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("Score"), null, CompareOp.EQUAL,
            //        Bytes.toBytes("90"));
          //  filters.add(filter2);

           /* Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
                    Bytes.toBytes("ccc"));
            filters.add(filter3);*/
            FilterList filterList1 = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            //System.out.println(rs);        
    	    try 
    	    {
 
    	         for (Result r : rs) {
    	             for (Cell cell : r.listCells()) {
    	            	 String row = new String(CellUtil.cloneRow(cell));
    	            	 if(!row.equals("1")){//no care number one row
	      	            	 String find = new String(CellUtil.cloneQualifier(cell));
	    	            	 if(find.equals("movie_title"))//show title_name
	    	            	 {
	    	            		 sum++;
		    	                 //System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
		    	                 //System.out.println("Timetamp(时间戳):"+cell.getTimestamp()+" ");
		    	                 //System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
		    	                 //System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
		    	                 //System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
	    	            	 }	            		 
    	            	 }
    	             }
    	         }
    	         //System.out.println("查看IMDB评分大于等于"+range+"的电影数量：:" + sum);   	       
    	    }finally {
   	         	close();
        }
        }catch(Exception e){
        	e.printStackTrace();
        }
        return sum;
        
   }
    //USA UK New Zealand Canada Australia Belgium Japan Germany China France New Line Mexico Spain 
    public static int QueryCountrySum(String tableName, String country) {
    	int sum = 0;
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            //Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("imdb_score"), CompareOp.GREATER_OR_EQUAL,
            //        Bytes.toBytes(String.valueOf(range)));
           // filters.add(filter1);
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("country"),CompareOp.EQUAL,
                    Bytes.toBytes(country));
            filters.add(filter2);

           /* Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
                    Bytes.toBytes("ccc"));
            filters.add(filter3);*/
            FilterList filterList1 = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            //System.out.println(rs);        
    	    try 
    	    {
 
    	         for (Result r : rs) {
    	             for (Cell cell : r.listCells()) {
    	            	 String row = new String(CellUtil.cloneRow(cell));
    	            	 if(!row.equals("1")){//no care number one row
	      	            	 String find = new String(CellUtil.cloneQualifier(cell));
	    	            	 if(find.equals("movie_title"))//show title_name
	    	            	 {
	    	            		 sum++;
		    	                 //System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
		    	                 //System.out.println("Timetamp(时间戳):"+cell.getTimestamp()+" ");
		    	                 //System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
		    	                 //System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
		    	                 //System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
	    	            	 }	            		 
    	            	 }
    	             }
    	         }	       
    	    }finally {
   	         	close();
        }
        }catch(Exception e){
        	e.printStackTrace();
        }
        return sum;
   }  
    /**
     * 根据表名查找表信息
     */
    public static int QuerySomeScore(String tableName, int range ,String country) {
    	int sum = 0;
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("imdb_score"), CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes(String.valueOf(range)));
            filters.add(filter1);
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("country"),CompareOp.EQUAL,
                    Bytes.toBytes(country));
            filters.add(filter2);

           /* Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
                    Bytes.toBytes("ccc"));
            filters.add(filter3);*/
            FilterList filterList1 = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            //System.out.println(rs);        
    	    try 
    	    {
 
    	         for (Result r : rs) {
    	             for (Cell cell : r.listCells()) {
    	            	 String row = new String(CellUtil.cloneRow(cell));
    	            	 if(!row.equals("1")){//no care number one row
	      	            	 String find = new String(CellUtil.cloneQualifier(cell));
	    	            	 if(find.equals("movie_title"))//show title_name
	    	            	 {
	    	            		 sum++;
		    	                 //System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
		    	                 //System.out.println("Timetamp(时间戳):"+cell.getTimestamp()+" ");
		    	                 //System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
		    	                 //System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
		    	                 //System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
	    	            	 }	            		 
    	            	 }
    	             }
    	         }	       
    	    }finally {
   	         	close();
        }
        }catch(Exception e){
        	e.printStackTrace();
        }
        return sum;
        
   }  
    public static int QueryYearSum(String tableName,  String year) {//String country,
    	int sum = 0;
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            //Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("imdb_score"), CompareOp.GREATER_OR_EQUAL,
            //        Bytes.toBytes(String.valueOf(range)));
           // filters.add(filter1);
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("title_year"),CompareOp.EQUAL,
                    Bytes.toBytes(year));
            filters.add(filter2);

           /* Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
                    Bytes.toBytes("ccc"));
            filters.add(filter3);*/
            FilterList filterList1 = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);     
    	    try 
    	    {
 
    	         for (Result r : rs) {
    	             for (Cell cell : r.listCells()) {
    	            	 String row = new String(CellUtil.cloneRow(cell));
    	            	 if(!row.equals("1")){//no care number one row
	      	            	 String find = new String(CellUtil.cloneQualifier(cell));
	    	            	 if(find.equals("movie_title"))//show title_name
	    	            	 {
	    	            		 sum++;
	    	            	 }	            		 
    	            	 }
    	             }
    	         }	       
    	    }finally {
   	         	close();
        }
        }catch(Exception e){
        	e.printStackTrace();
        }
        return sum;
        
   }  
    public static int QueryYearSumByCountry(String tableName, String year, String country,int range ) {
    	int sum = 0;
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("imdb_score"), CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes(String.valueOf(range)));
            filters.add(filter1);
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("country"),CompareOp.EQUAL,
                    Bytes.toBytes(country));
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("title_year"), CompareOp.EQUAL,
                    Bytes.toBytes(year));
            filters.add(filter3);
            
            FilterList filterList1 = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            //System.out.println(rs);        
    	    try 
    	    {
 
    	         for (Result r : rs) {
    	             for (Cell cell : r.listCells()) {
    	            	 String row = new String(CellUtil.cloneRow(cell));
    	            	 if(!row.equals("1")){//no care number one row
	      	            	 String find = new String(CellUtil.cloneQualifier(cell));
	    	            	 if(find.equals("movie_title"))//show title_name
	    	            	 {
	    	            		 sum++;
		    	                 //System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
		    	                 //System.out.println("Timetamp(时间戳):"+cell.getTimestamp()+" ");
		    	                 //System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
		    	                 //System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
		    	                 //System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
	    	            	 }	            		 
    	            	 }
    	             }
    	         }	       
    	    }finally {
   	         	close();
        }
        }catch(Exception e){
        	e.printStackTrace();
        }
        return sum;
        
   }  
    public static void getData(String tableName)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
       ResultScanner scanner = table.getScanner(scan);
        
        for(Result result:scanner)
        {
            showCell((result));
        }
        close();
    }
    /**
     * 查找数据中电影来自什么年份
     */
     public static void getYear(String tableName)throws  IOException{
         init();
         Table table = connection.getTable(TableName.valueOf(tableName));
         Scan scan = new Scan();
         ResultScanner scanner = table.getScanner(scan);
         List<String> Year = new ArrayList<String>();
         for(Result result:scanner)
         {
         	Cell[] cells = result.rawCells();
         	for(Cell cell:cells){
 	        	String row = new String(CellUtil.cloneRow(cell));
 	        	if(!row.equals("1")){
  	            	String find = new String(CellUtil.cloneQualifier(cell));
 	            	if(find.equals("title_year")){//show title_name
 	            		String find_year = new String(CellUtil.cloneValue(cell));
 	            		if(!Year.contains(find_year)){
 	            			Year.add(find_year);
 	            		}            		
 	            	}        		
 	        	}
         	}
         		
         }
         close();
         for(String a:Year){        	
         	System.out.print(a);  
         	int sum = QueryYearSum("movie",a);
         	System.out.println(" : " + sum);
         }
     } 

     
    
   /**
    * 查找数据中电影来自多少各国家和占比
    */
    public static void getCountry(String tableName)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        List<String> country = new ArrayList<String>();
        for(Result result:scanner)
        {
        	Cell[] cells = result.rawCells();
        	for(Cell cell:cells){
	        	String row = new String(CellUtil.cloneRow(cell));
	        	if(!row.equals("1")){
 	            	String find = new String(CellUtil.cloneQualifier(cell));
	            	if(find.equals("country")){//show title_name
	            		String find_country = new String(CellUtil.cloneValue(cell));
	            		if(!country.contains(find_country)){
	            			country.add(find_country);
	            		}            		
	            	}        		
	        	}
        	}
        		
        }
        close();
        for(String a:country){        	
        	System.out.print(a);  
        	int sum = QueryCountrySum("movie",a);
        	System.out.println(" : " + sum);
        }
    }
    
    /**
     * 格式化输出
     * @param result
     */
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
        	String row = new String(CellUtil.cloneRow(cell));
        	if(!row.equals("1")){
	            System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
	            System.out.println("Timetamp(时间戳):"+cell.getTimestamp()+" ");
	            System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
	            System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
	            System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
	            System.out.println();
        	}
        }
    }
    
    
    
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
    	Data_Analyze t =new Data_Analyze();
        //System.out.println("请输入要查看的表名");
        //Scanner scan = new Scanner(System.in);
        //String tableName=scan.nextLine();
        System.out.println("信息如下：");
        /**
         * IMDB电影评分大于9.0的分数以及电影名
         */        
        t.QueryTopMovie("movie");
        
        /**
         * IMDB电影评分范围
         */
        HashMap map = new HashMap();
        for(int i=1;i<10;i++){     	
        	int sum = t.QueryScore("movie",i);  
        	map.put(i,sum);
        }
        for(int i=1;i<10;i++){ 
         	if(i == 9){
        		System.out.println("IMDB评分大于等于"+i+"的电影数量:"+map.get(9));
        	}   
         	else{
	        	int litter_num= Integer.parseInt(map.get(i+1).toString());
	        	int big_num= Integer.parseInt(map.get(i).toString());     	
	        	int res = big_num - litter_num;
	        	System.out.println("IMDB评分在"+i+"~"+(i+1)+"之间的电影数量:"+res);
         	}
        }
        System.out.println();
        
        
        /**
         * 电影country
         */
        //USA UK New Zealand Canada Australia Belgium Japan Germany China France New Line Mexico Spain        
        System.out.println("数据中电影来自多少个国家和数量");
        t.getCountry("movie");
        
        /**
         * USA电影
         */
        
        HashMap map1 = new HashMap();
        for(int i=1;i<10;i++){     	
        	int sum = t.QuerySomeScore("movie",i,"USA");  
        	map1.put(i,sum);
        }
        for(int i=1;i<10;i++){ 
         	if(i == 9){
        		System.out.println("USA:IMDB评分大于等于"+i+"的电影数量:"+map1.get(9));
        	}   
         	else{
	        	int litter_num= Integer.parseInt(map1.get(i+1).toString());
	        	int big_num= Integer.parseInt(map1.get(i).toString());     	
	        	int res = big_num - litter_num;
	        	System.out.println("USA:IMDB评分在"+i+"~"+(i+1)+"之间的电影数量:"+res);
         	}
        }
        System.out.println();
        /**
         * UK电影
         */
        
        HashMap map2 = new HashMap();
        for(int i=1;i<10;i++){     	
        	int sum = t.QuerySomeScore("movie",i,"UK");  
        	map2.put(i,sum);
        }
        for(int i=1;i<10;i++){ 
         	if(i == 9){
        		System.out.println("UK:IMDB评分大于等于"+i+"的电影数量:"+map2.get(9));
        	}   
         	else{
	        	int litter_num= Integer.parseInt(map2.get(i+1).toString());
	        	int big_num= Integer.parseInt(map2.get(i).toString());     	
	        	int res = big_num - litter_num;
	        	System.out.println("UK:IMDB评分在"+i+"~"+(i+1)+"之间的电影数量:"+res);
         	}
        }
        System.out.println();       
        
        
        System.out.println("数据中电影来自什么年份");
        t.getYear("movie");
        System.out.println();  
        
        /**
         * USA电影by years
         */
        for(int y=2009;y<2017;y++){
	        HashMap map3 = new HashMap();
	        for(int i=1;i<10;i++){     	
	        	int sum = t.QueryYearSumByCountry("movie",String.valueOf(y),"USA",i);  
	        	map3.put(i,sum);
	        }
	        for(int i=1;i<10;i++){ 
	         	if(i == 9){
	        		System.out.println(y+"年 USA:IMDB评分大于等于"+i+"的电影数量:"+map3.get(9));
	        	}   
	         	else{
		        	int litter_num= Integer.parseInt(map3.get(i+1).toString());
		        	int big_num= Integer.parseInt(map3.get(i).toString());     	
		        	int res = big_num - litter_num;
		        	System.out.println(y+"年 USA:IMDB评分在"+i+"~"+(i+1)+"之间的电影数量:"+res);
	         	}
	        }
	        System.out.println();       	
        }

 
    }

}
