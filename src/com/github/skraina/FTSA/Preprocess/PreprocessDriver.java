package com.github.skraina.FTSA.Preprocess;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;


public class PreprocessDriver
{
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
/*		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2)
		{
			System.err.println("Usage: Preprocess Twitter data <in> <out>");
			System.exit(2);
		}
*/		
		//@SuppressWarnings("deprecation")
		//Job job = new Job(conf, "Preprocess Twitter JSON Data");
		Job job = Job.getInstance(conf);
		job.setJobName("Preprocess Twitter JSON Data");
		job.setJarByClass(PreprocessDriver.class);
		job.setMapperClass(PreprocessMapper.class);
		//job.setReducerClass(AvgMonHrsPerDeptReducer.class);
		
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);	
		Path inPath = new Path("hdfs://localhost:9000/FTSA/Twitter/TwitterData.json");
		//Path inPath = new Path("hdfs://localhost:9000/Twitter/positive-words.txt");
		Path outPath = new Path("hdfs://localhost:9000/FTSA_output/");
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
        job.addCacheFile(new Path("/jars/argo-5.1.jar").toUri());
        //org.apache.hadoop.filecache.DistributedCache.addFileToClassPath(new Path("/jars/jdom.jar"), job.getConfiguration(), FileSystem.get(conf));
        
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}