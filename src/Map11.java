/*
 * Mapper:> ClusterNo: Points Belonging to it
 * 1: <1,1> <2,2>
 */


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;



public class Map11 extends TableMapper<IntWritable, Text>{
	
	static int k=0;

	static double centers[][] = new double[k][10];

	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
//		System.out.println("Mapper k: "+k);
		System.out.println("Hi");
		Path centersPath = new Path(conf.get("centroid.path"));
/*		k=Program.k;
		int lk = Integer.parseInt(conf.get("k"));
		if(lk!=0)
			k=lk;
*/		URI uri = null;
		try {
			uri = new URI(conf.get("fs.default.name","."));
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileSystem fs = FileSystem.get(uri,conf);
         try{
       	  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centersPath)));
       	  String line = br.readLine();
       	  int index =0;
       	  while (line!= null){
	  		  String[] values = line.split("\t");
	  		  for(int i=0;i<values.length;i++){
	  			Double di = Double.parseDouble(values[i]);
	  			centers[index][i] = di.doubleValue();
	  			}
	  		index++;
	  		line=br.readLine();
	  	
	  		k++;
       	  }
        br.close();
       }  
     catch(Exception e){}		
	}
	
	@Override
	public void map(ImmutableBytesWritable row, Result Values, Context context) throws IOException, InterruptedException{
			
		double [] different = new double[k]; //How much different each cluster mean is from this row
		double dMin = 1000;
    	int index=0;
    	
		//Creating the vector//
    	double [] dVector= new double[10];
    	// Area:X0	Area:X4	Area:X5	Area:X8	Area:X9	Property:X1	Property:X2	Property:X3	Property:X6	Property:X7
 
    	for(KeyValue kv : Values.raw()){
    		Double x = Bytes.toDouble(kv.getValue(),0);
    		String qualifier = Bytes.toString(kv.getQualifier());
    		int vIndex = Integer.parseInt(qualifier.substring(1));
    		dVector[vIndex] = x.doubleValue();
    	}
		
		//Finding Difference wrt each cluster mean
 		double [] norm = {0.62d,514d,294d,110d,3.5d,2d,0.4d,2d,28d,30d}; 
		double sum=0,featureDiff;
		for(int i=0; i<k; i++){
        	for(int j=0; j<10; j++){
        		System.out.println(i + " : " + j);
        		featureDiff = Math.pow((centers[i][j]         				
        				- dVector[j])/norm[j],2);
        		sum = sum + featureDiff;
        	}
        	different[i] = sum;
        	sum=0;
		}
   
		for(int i=0;i<k;i++)
        	if(different[i]<dMin){
        		dMin=different[i];
        		index=i; 		//Finding the closest cluster center to this row
        	}
		StringBuilder outVector = new StringBuilder();
		DecimalFormat df = new DecimalFormat("#.###");
		for(int i=0;i<9;i++){
			outVector.append(df.format(dVector[i]) + "#");
		}
		
		outVector.append(df.format(dVector[9]));
		Text out = new Text(outVector.toString());
		context.write(new IntWritable(index),out);
	}
}

