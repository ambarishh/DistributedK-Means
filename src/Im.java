import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Im {

    private static Configuration conf1 = null;
    /**
     * Initialization
     */
    static {
    	
       conf1 = HBaseConfiguration.create();
       
    }
 
    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf1);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }
	
    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,String family, String qualifier, Double value) throws Exception {
        try {
            HTable table = new HTable(conf1, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
         //   System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
  /*  *//**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf1);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }
    
    /*Clear Table
     * 
     */
    
    public static void clearTable(String tableName) throws Exception{
    	deleteTable(tableName);
    	String[] familys = { "Area", "Property" };
    	creatTable(tableName, familys);
    }
    
public static void main(String[] args) throws Exception {
	try{	
	String tablename = "AmboDead";
    String[] familys = { "Area", "Property" };
    String filename = "dataset.txt"; 
 
   /* URI uri = new URI(conf1.get("fs.default.name","."));
    FileSystem fs = FileSystem.get(uri,conf1);*/
    
   /* if(!fs.exists(new Path(filename)))
    	System.out.println("No file exists");
    else{
    	System.out.println("File exists");
     }
    */
//    Im.fillTable(new File(filename), tablename);
    Im.creatTable(tablename, familys);
	}
	catch(Exception e){
		e.printStackTrace();
		
	}
/*     Importer.creatTable(tablename, familys);
 * Importer.fillTable(filename, tablename);
Importer.clearTable(tablename);
*/ 
	}


public static void fillTable(File filename, String tablename){
try{
   int count=1;
    File data = filename;
    BufferedReader br = new BufferedReader(new FileReader(data));
    String family;
    System.out.println("Begining filling ...");
    while(br.ready()){
    	String line = br.readLine();
    	String[] vector = line.split("\t");
    	for(int i=0; i<vector.length;i++){
    		if(i==0 || i==4 || i==5 || i==8 || i==9)
    			family = "Area";
    		else
    			family = "Property";
    		Im.addRecord(tablename, "row"+count , family, "X"+i, Double.parseDouble(vector[i]));	
   // 		System.out.print(vector[i] + " ");
    	}
   // 	System.out.println();
    	count++;
    }
    br.close();
    System.out.println("Table Filled with data ...");
 }
   catch(Exception e){
	    e.printStackTrace();
   		}
	}
}
