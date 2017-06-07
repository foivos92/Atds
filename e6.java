
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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


public class e6 {
	  
	public static class TokenizerMapperUsers extends Mapper<Object, Text, Text, Text>{
           private final static Text zero = new Text(" ");
           private final static Text result = new Text(" ");
           private Text word = new Text(" ");

           public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	   StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
        	   Text id,q,t,date,time,rank,url;
        	 		id = new Text (itr.nextToken());
   					q=new Text (itr.nextToken());
   					t=new Text (itr.nextToken());
    	        	//StringTokenizer itr2 = new StringTokenizer(q.toString(), " ");

   	        	   StringTokenizer itr1 = new StringTokenizer(q.toString(), " ");
   	        	   result.set(id+"@@"+t);//morfi result : id@@date time 
   	        	   while(itr1.hasMoreTokens()){
   	        		   word.set(itr1.nextToken().toLowerCase());
   	        		   context.write(word, result);

   	        	   }

        	  
           }
	}
	
	 public static class TokenizerMapperWiki extends Mapper<Object, Text, Text, Text>{

//		  private final static LongWritable one = new LongWritable(1);
		  private Text word = new Text();
		  private final static Text result = new Text("wiki");
		  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  	StringTokenizer itr1=new StringTokenizer(value.toString(),"\t");
			  	while (itr1.hasMoreTokens()){
				  StringTokenizer itr = new StringTokenizer(itr1.nextToken().toString(), "_");
				
				  while (itr.hasMoreTokens()) {
				        word.set(itr.nextToken().toLowerCase());
				        context.write(word, result);
				      }
			  	}
		  	}
	 }
	 
	 public static class ReducerFound extends Reducer<Text,Text,Text,Text> {
		 private Text result = new Text();
		
		 public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			  Text found=new Text("found");
			  Text notfound= new Text("notfound");
			  int flag1=0,flag2=1;
			  long countW=0,countOther=0;
		      List<String> al = new ArrayList<String>();

			  for (Text val : values) {
			      al.add(val.toString());
				
				 if (val.toString().equals("wiki")){
					  flag1=1;
					  //countW++;
				  }
				  else if (!val.toString().equals("wiki")){
					  flag2=1;
					  countOther++;
				  }
				 // if (flag1 && flag2){
				//	  break;
				 // }
			  }
			long i=0;
			String help= new String(" ");
			 Iterator<String> itr = al.iterator();
			  //steile poses fores egine auto to erwtima se ka8e periptwsi
			  //to keyword mporei na vre8ei sto  wiki ws title 
			  if (flag1==1 && flag2==1){
				  while (itr.hasNext()){
					  help=itr.next();
					//an den isoutai me w tote steile found 
					  if(!help.equals("wiki")){
						  context.write(new Text(help), found);
					  }
					//i++;
				  }
			  }
			  //to keyword den mporei na vre8ei sto wiki   
			  else if(flag1==0  && flag2==1) {
				  while (itr.hasNext()){
						help=itr.next();
						  context.write(new Text(help), notfound);
						//i++;
				  }
			  }
		  }
	 }
	 
	 public static class TokenizerMapperForwarder extends Mapper<Object, Text, Text, Text>{

		  private Text word = new Text(" ");
		  private Text result = new Text(" ");
		  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  	StringTokenizer itr=new StringTokenizer(value.toString(),"\t");
		        word.set(itr.nextToken().toString());
		        result.set(itr.nextToken().toString());
		        context.write(word, result);
				     
		  }
	 }
	 
	 public static class ReducerFilter extends Reducer<Text,Text,Text,Text> {
		 private Text result = new Text();
		 public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			  Text found=new Text("found");
			  Text notfound= new Text("notfound");
			  for(Text val : values){
				  if(val.toString().equals("found")){
					 context.write(new Text("key"), found);
					//context.write(key,found);  
					break;
				  }
				  else if (val.toString().equals("notfound")){
					  context.write(new Text("key"), notfound);
					//context.write(key,notfound); 
					 break;
				  }
			  }
		  }
	 }
	 
	 public static class ReducerFinalSum extends Reducer<Text,Text,Text,DoubleWritable> {

		 private Text result = new Text();
		 public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			  Text found=new Text("found");
			  Text notfound= new Text("notfound");
			  long sumFound=0,sumNotFound=0;
			  for(Text val : values){
				  if(val.toString().equals("found")){
					  //context.write(new Text("key"), found);
					  sumFound++;
				  }
				  else if (val.toString().equals("notfound")){
					  //context.write(new Text("key"), notfound);
					  sumNotFound++;
				  }
			  }
			  float Qexists=0,Qnotexists=0;
			  Qexists=(((float)(sumFound))/((float)(sumFound)+(float)(sumNotFound)))*100;
			  Qnotexists=(((float)(sumNotFound))/((float)(sumFound)+(float)(sumNotFound)))*100;
			  
			  context.write(new Text("Qexist"), new DoubleWritable(Qexists));
			  context.write(new Text("Qnot_exit"), new DoubleWritable(Qnotexists));
		  }
	 }
	
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    if (otherArgs.length < 3) {
		      System.err.println("Usage:find Qs <in> [<in>...] <out>");
		      System.exit(2);
		    }
		    String myPath="/tmp/tmpResults";
		    String myPath1="/tmp/tmpResults1";
		    Job job1 = new Job(conf, "join");
		    Job job2 = new Job(conf, "filter");
		    Job job3 = new Job(conf, "calc sum");

		    
		    job1.setJarByClass(e6.class);
		    //job.setMapperClass(TokenizerMapperUsers.class);
		 
		    job1.setReducerClass(ReducerFound.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(Text.class);
		    MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, TokenizerMapperUsers.class);
		    MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, TokenizerMapperWiki.class);
		    FileOutputFormat.setOutputPath(job1, new Path(myPath));
		    //FileOutputFormat.setOutputPath(job1,new Path(otherArgs[otherArgs.length-1]));
		    job1.waitForCompletion(true);
		    
		    job2.setJarByClass(e6.class);
		    job2.setMapperClass(TokenizerMapperForwarder.class);
		    job2.setReducerClass(ReducerFilter.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job2, new Path(myPath));

		    FileOutputFormat.setOutputPath(job2, new Path(myPath1));
		    job2.waitForCompletion(true);

//		    job2.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
		    job3.setJarByClass(e6.class);
		    job3.setNumReduceTasks(1);
		    job3.setMapperClass(TokenizerMapperForwarder.class);
		    job3.setReducerClass(ReducerFinalSum.class);
		    job3.setOutputKeyClass(Text.class);
		    job3.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job3, new Path(myPath1));

		    FileOutputFormat.setOutputPath(job3,new Path(otherArgs[otherArgs.length - 1]));
		    System.exit(job3.waitForCompletion(true) ? 0 : 1);
		  }
	 
	 
	}
		 
	
	
	


