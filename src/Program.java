/*
 * The driver program to implement kmeans
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Program {
	
	private static Configuration conf = null;
	static int k;
	static {
    	conf = HBaseConfiguration.create();
      }
    
    public static void main(String args[])throws Exception{
    	String txt_ip,Htable = null,initialPath = null,centersPath = null;
    	
    	System.out.println("Start ...");
    	
    	if(args.length<5){
    		System.out.println("Input Format: Txt I/p HTable K InitialPath OutputPath");
    		txt_ip = "dataset.txt";
    		Htable = "AmboEnergy";
    		k=5;
    		initialPath = "/opt/hadoop/tmp/Prob/center.txt"; //Initial file path
    		centersPath = "/opt/hadoop/tmp/Prob/centerOut4.txt"; //Output path
    		//System.exit(1);
    	  	/*Filling the table with data    	 */
    	 	Im.fillTable(new File(txt_ip), Htable);
    	}
 //   	dataset.txt Energy 5 /opt/hadoop/tmp/Prob/center.txt /opt/hadoop/tmp/Prob/centerOut.txt
 //		newData.txt AmboEnergy2 5 /opt/hadoop/tmp/Prob/center.txt /opt/hadoop/tmp/Prob/centerOut5.txt
    	
    	else{
    		System.out.println("Reading from command line...");
    		txt_ip = args[0];
    		Htable = args[1];
    		k = Integer.parseInt(args[2]);
    		initialPath = args[3];
    		centersPath = args[4];
    	  	/*Filling the table with data    	 */
    		
    	 	Im.fillTable(new File(txt_ip), Htable);
    	}
    	
  
    	
    	Job job = new Job();
        int iteration = 0;    
        
        //This is the danger zone //
     //  conf.set("fs.default.name", "hdfs://ambo.hadoop.local:9000");
         conf.set("doIterate", "true");
         conf.set("k",k+"");
        // //
        
    	URI uri = new URI(conf.get("fs.default.name","."));
        FileSystem fs = FileSystem.get(uri,conf);
       
        //Copy the contents of file from initial input path to output path
        //1.Read contents
        StringBuilder temp = new StringBuilder();
        
        if(!fs.exists(new Path(initialPath)))
        	System.out.println("No file exists");
        
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(initialPath))));
        String line = br.readLine();
        while(line!=null){
        	temp.append(line + "\n");
        	line=br.readLine();
        }
        br.close();
        //2.Write contents
        //Create File
       /* if(fs.create(new Path(centersPath)) != null)
        	System.out.println("File Created");*/
        
    /*    System.out.println("Writing to " + uri + centersPath ); */
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(centersPath),true)));
		bw.write(temp.toString());
		bw.close();
        
        conf.set("centroid.path", centersPath);
        
    //   initializeCenters(conf,fs,new Path(conf.get("centroid.path")));
        

        while(conf.get("doIterate") != "false" && iteration<(k-1)){
             job = new Job(conf);
             job.setJobName("HBase KMeans Clustering " + iteration);
             String output;
        
             int nextIter = iteration + 1;
             output = "/opt/hadoop/tmp/Prob/center"+ nextIter + ".txt";; // setting the output file
             
             Path out = new Path(output);
             if (fs.exists(out))
                 fs.delete(out, true);
             FileOutputFormat.setOutputPath(job, out); // setting the output files for the job
             job.setOutputKeyClass(IntWritable.class);
             job.setOutputValueClass(Text.class);
             Scan scan = new Scan();
             TableMapReduceUtil.initTableMapperJob(Htable, scan, Map11.class, IntWritable.class, Text.class, job);
             job.setReducerClass(Reduce11.class); 
             job.setJarByClass(Program.class);
        
             iteration++;     
             job.setNumReduceTasks(1); 
             job.waitForCompletion(true);
         }
        Im.clearTable(Htable);
        System.out.println("Finished...");
        System.exit(0);
    }
  
    public static void initializeCenters(Configuration conf, FileSystem fs, Path centersPath) throws IOException{
    	
    	int total =0;
		int count =1;
		File data = new File("dataset.txt");
		StringBuilder o = new StringBuilder();
		
        BufferedReader br = new BufferedReader(new FileReader(data));
        while(br.ready() && total<k){	 
        	String line = br.readLine();
        	if(count%20==0){
        		o.append(line + "\n");
        		total++;
        	}
	        count++;
        }
        br.close();
        BufferedWriter hbw = new BufferedWriter(new OutputStreamWriter(fs.create(centersPath, true)));
		hbw.write(o.toString());
		hbw.close();
 
   }
}
