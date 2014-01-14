


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class Reduce11 extends Reducer<IntWritable, Text, IntWritable, Text>{
	static HashMap<Integer,String> hm = new HashMap<Integer,String>();
	static int count=0;
	static int k=0;
//	= Program.k;

	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();	
		Path centersPath = new Path(conf.get("centroid.path"));
/*		int lk = Integer.parseInt(conf.get("k"));
		if(lk!=0)
			k=lk;*/
		URI uri = null;
		try {
			uri = new URI(conf.get("fs.default.name","."));
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileSystem fs = FileSystem.get(uri,conf);
		System.out.println("Reducer Setup Enter");
        try{
      	  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centersPath)));
      	  String line = br.readLine();
      	  int index =0;
      	  while (line!= null){
//	  		  System.out.println("Reducer Line: "+ line);
	  		  hm.put(new Integer(index), line);
	  		  index++;
	  		  line=br.readLine();
	  		  k++;
      	   }
         br.close();
        }  
    	catch(Exception e){}
	}
	
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> Values, Context context) throws IOException, InterruptedException{
		
		ArrayList<Double> [] centerList = new ArrayList[10];
		double [] avg = new double[10];
	
		for(int i=0;i<10;i++)
			centerList[i] = new ArrayList<Double>();
		
		for(Text t:Values){
			String line = t.toString();
			String [] features = line.split("#");
			for(int i=0; i<features.length;i++)
				centerList[i].add(Double.parseDouble(features[i]));
		}
		for(int i=0;i<centerList.length;i++){
			double sum=0;
			Iterator<Double> it = centerList[i].iterator();
			while(it.hasNext()){
				sum = sum + it.next().doubleValue();
			}
			avg[i] = sum/centerList[i].size();
		}
		DecimalFormat df = new DecimalFormat("#.###");
		StringBuilder out = new StringBuilder();
		for(int i=0;i<9;i++)
			out.append(df.format(avg[i]) + "\t");
		out.append(df.format(avg[9]));
		
//		System.out.println("Reducer O/p: " + key.get() + " : " + out.toString());
		int centroid = key.get();
		
		String oldValue = hm.get(centroid);
		String newValue = out.toString();
		if(oldValue.equals(newValue)){
			count++;
			System.out.println("Matched");
		}
		hm.put(new Integer(centroid), out.toString());
		//		for(IntWritable v:Values)
//			sum+=v.get();
		context.write(key, new Text(out.toString()));
		/*//To Write to a table
		Put put = new Put(key.get());
        put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
        System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.get()), sum));
        context.write(key, put);*/
	}
	
	@Override
	public void cleanup(Context context) throws IOException{
		StringBuilder outCentroids = new StringBuilder();
		Set<Integer> s = hm.keySet();
		Iterator<Integer> it = s.iterator();
		System.out.println("O/P to next Iteration");
		while(it.hasNext()){
			String str = hm.get(it.next());
			System.out.println(str);
			outCentroids.append(str + "\n");
		}
		
		//Re Store the centers file
		Configuration conf = context.getConfiguration();	
		Path centersPath = new Path(conf.get("centroid.path"));
		URI uri = null;
		try {
			uri = new URI(conf.get("fs.default.name","."));
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileSystem fs = FileSystem.get(uri,conf);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(centersPath, true)));
		bw.write(outCentroids.toString());
		bw.close();
		if(count==k)
			conf.set("doIterate", "false");
	}
	
}
