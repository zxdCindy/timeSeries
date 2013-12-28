package question4;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import sun.util.locale.StringTokenIterator;

/**
 * Moving average for Time series
 * @author cindyzhang
 *
 */
public class TimeSeries {
	
	public static class TimeSeriesMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable>{
		private int N = 0;//how many numbers we will handle in time series processing
		Queue<Integer> inputQue = new LinkedList<Integer>();
		String timeStamp = null;
		Integer timeValue = null;
		Text outputKey = new Text();
		IntWritable outputValue = new IntWritable();
		
		@Override
		public void setup(Context context){
			N = Integer.valueOf(context.getConfiguration().get("VALUEOFN"));
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] tokens = value.toString().split(",");
			timeStamp = tokens[0];
			timeValue = Integer.valueOf(tokens[1]);
			
			//When received N values, process these data by mapreduce job
			if(inputQue.size() == N){
				inputQue.poll();
				inputQue.add(timeValue);
				for(Integer num: inputQue){
					outputKey.set(timeStamp);
					outputValue.set(num);
					context.write(outputKey, outputValue);
				}
			}
			else
				inputQue.add(timeValue);
			
		}
	}
	
	/**
	 * Sort the output data by time stamp
	 *
	 */
	public static class TimeSeriesPartitioner extends Partitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int numReducer) {		
			int year = Integer.valueOf(key.toString().substring(0, 4));
			int num = year % numReducer;
			if(year>=2000 && year<=2002)
				num = 0;
			else if(year>=2003 && year<=2005)
				num = 1;
			else if(year>=2006 && year<=2008)
				num = 2;
			else if(year>=2009 && year<=2011)
				num = 3;
			else if(year>=2012 && year<=2014)
				num = 4;
			return num;
		}
		
	}
	
	/**
	 * Calculate the moving average
	 *
	 */
	public static class TimeSeriesReducer extends 
		Reducer<Text, IntWritable, Text, DoubleWritable>{
		private int N = 0;
		DoubleWritable outputValue = new DoubleWritable();		

		@Override
		public void setup(Context context){
			N = Integer.valueOf(context.getConfiguration().get("VALUEOFN"));
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value: values){
				sum += value.get();
			}
			double movingAvg = sum/(N * 1.0);
			outputValue.set(movingAvg);
			context.write(key, outputValue);
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		//Path inputDir = new Path("/Users/cindyzhang/Desktop/time/input.txt");
		//Path outputDir = new Path("/Users/cindyzhang/Desktop/output");
		Configuration conf = new Configuration();
		conf.set("VALUEOFN", "5");
		Job job = new Job(conf, "TimeSeries");
		job.setJarByClass(TimeSeries.class);
		job.setMapperClass(TimeSeriesMapper.class);
		job.setPartitionerClass(TimeSeriesPartitioner.class);
		job.setReducerClass(TimeSeriesReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);
		job.setNumReduceTasks(5);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);		
		job.waitForCompletion(true);
	}

}
