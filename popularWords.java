import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import java.lang.Long;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class popularWords {

	 public static class TokenizerMapperOne extends Mapper<Object, Text, Text, LongWritable>{

	  private final static LongWritable one = new LongWritable(1);
	  private Text word = new Text();

	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  StringTokenizer itr1=new StringTokenizer(value.toString(),"\t");
		  itr1.nextToken();			  
		   StringTokenizer itr = new StringTokenizer(itr1.nextToken().toString(), " ");
			
			  while (itr.hasMoreTokens()) {
			        word.set(itr.nextToken().toLowerCase());
			        context.write(word, one);
			      }
		  
	  	}
}

	public static class IntSumReducerOne extends Reducer<Text,LongWritable,Text,LongWritable> {
	  private LongWritable result = new LongWritable();
	
	  public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
	    long sum = 0,sumUsers=0;
	    for (LongWritable val : values) {
	      sum += 1;
	      
	    }
	  	  result.set(sum);
	  	  context.write(key, result);
	  }
	}
	
	
	 public static class TokenizerMapperTwo extends Mapper<Object, Text, LongWritable, Text>{

		  private final static LongWritable sum = new LongWritable();
		  private Text word = new Text();
		  //antistrofi tou key-value
		  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  StringTokenizer itr=new StringTokenizer(value.toString());
				        word.set(itr.nextToken());
			  			sum.set(Long.parseLong(itr.nextToken()));
				        context.write(sum,word);
			  }
		 }
	

		public static class IntSumReducerTwo extends Reducer<LongWritable,Text,Text,LongWritable> {
		  //private IntWritable result = new IntWritable();
		
		  public void reduce(LongWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		    int sum = 0,sumUsers=0;
		    for (Text val : values) {
		    	context.write(val,key);
		      
		    }
		  }
	}
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage:popular words in AOL <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    String myPath="/tmp/tmpResults";
	    Job job = new Job(conf, "Find the sum for each unique word");
	    Job job2 = new Job(conf, "Sort the word with decr order");
	    
	    
	    job.setJarByClass(popularWords.class);
	    job.setMapperClass(TokenizerMapperOne.class);
	    job.setReducerClass(IntSumReducerOne.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    
	    job2.setJarByClass(popularWords.class);
	    job2.setMapperClass(TokenizerMapperTwo.class);
	    job2.setReducerClass(IntSumReducerTwo.class);
	    //job2.setNumReduceTasks(1);
	    job2.setOutputKeyClass(LongWritable.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	    job2.setNumReduceTasks(1);
	    
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	   // p=new Path(myPath);
	    FileOutputFormat.setOutputPath(job, new Path(myPath));
	    job.waitForCompletion(true);
	    
	    FileInputFormat.addInputPath(job2, new Path(myPath));

	    FileOutputFormat.setOutputPath(job2,new Path(otherArgs[otherArgs.length - 1]));
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	  }
}

