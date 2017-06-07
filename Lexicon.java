import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by foivos on 28/3/2017.
 */
public class Lexicon extends Configured implements Tool{

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        //private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();
        //private final static LongWritable one = new LongWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "_");
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                word.set(str);
                context.write(word, new Text(""));
            }
        }
    }

    public  static  class Combo extends Reducer<Text, Text, Text, Text> {

        private Text word = new Text();

        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static class OneReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    private void unify(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path respath = new Path(path, "results");
        FSDataOutputStream out = fs.create(respath);
        for (int k = 0; k < 10; k++) {
            Path file = new Path(path, "part-r-0000"+ k);
            if (!fs.exists(file))
                throw new IOException("Output not found! " + k);
            BufferedReader br = null;
            try {
                br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
                String line;
                while ((line = br.readLine()) != null) {
                    out.writeBytes(line+"\n");
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Lexicon(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: Lexicon <in> [<in>...] <intermediate> <partition> <out>");
            System.exit(2);
        }

        //create result dirs
        //Path intermediate = new Path(otherArgs[otherArgs.length - 1],"/analysed/");
        Path intermediate = new Path(otherArgs[otherArgs.length -3 ]);
        //FileSystem.get(conf).mkdirs(intermediate);
        //Path partition_output = new Path(otherArgs[otherArgs.length - 1],"/partition/");
        Path partition_output = new Path(otherArgs[otherArgs.length - 2]);
        //FileSystem.get(conf).mkdirs(partition_output);
        //Path output = new Path(otherArgs[otherArgs.length - 1],"/output/");
        Path output = new Path(otherArgs[otherArgs.length - 1]);
        //FileSystem.get(conf).mkdirs(output);

        //create initial job
        Job initjob = Job.getInstance(conf, "Data Analysis");
        initjob.setNumReduceTasks(10);
        initjob.setJarByClass(Lexicon.class);
        initjob.setMapperClass(TokenizerMapper.class);
        initjob.setCombinerClass(Combo.class);
        initjob.setReducerClass(OneReducer.class);
        initjob.setOutputKeyClass(Text.class);
        initjob.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 3; ++i) {
            FileInputFormat.addInputPath(initjob, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(initjob, intermediate);
        boolean result = initjob.waitForCompletion(true);

        //create input for 2nd mapred
        //unify(intermediate, conf);

        //create main job
        Job job = Job.getInstance(conf, "Lexicon");
        job.setJarByClass(Lexicon.class);
        FileInputFormat.addInputPath(job, intermediate);

        //Partition File prep
        job.setNumReduceTasks(10);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partition_output);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);

        //Sampling
        InputSampler.Sampler<Text,Text> sampler = new InputSampler.RandomSampler<>(0.01, 10, 100);
        InputSampler.writePartitionFile(job, sampler);

        //Total Ordering
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        //Output
        FileOutputFormat.setOutputPath(job, output);
        boolean result1 = job.waitForCompletion(true);

        // Extras

        return (result && result1 ? 0 : 1);
    }
}

