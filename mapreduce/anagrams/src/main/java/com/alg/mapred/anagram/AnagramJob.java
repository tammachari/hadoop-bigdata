package com.alg.mapred.anagram;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramJob {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("AnagramJob");
		job.setJarByClass(AnagramJob.class);
		
		job.setMapperClass(AnagramMapper.class);
		job.setReducerClass(AnagramReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
			
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
