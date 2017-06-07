import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class over10 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

  //  private final static IntWritable usId = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
      
      if (itr.countTokens()==5 ){
    	  String link;
    	  String help=itr.nextToken();
    	  if (!help.equals("AnonID")){
	    	  IntWritable usId = new IntWritable(Integer.parseInt(help));
	    	  itr.nextToken();
	    	  itr.nextToken();
	    	  itr.nextToken();
	    	  link=itr.nextToken();
	    	  word.set(link);
	    	  context.write(word, usId);
    	  }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0,sumUsers=0;
      HashSet<Integer> hs = new HashSet<Integer>();

      for (IntWritable val : values) {
       //sum += 1;
        int user=val.get();
        if (!hs.contains(user)){
        	hs.add(user);
        	sumUsers+= 1;
        }
      }
      
      if (sumUsers>15){ 
	 result.set(sumUsers);
    	 context.write(key, result);
      }
   }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: over 10 visits in site <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "over10");
    job.setJarByClass(over10.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

