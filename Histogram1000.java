/**
 * Created by foivos on 28/3/2017.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.lang.Character;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Histogram1000 extends Configured implements Tool{
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, LongWritable>{

        //private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();
        private final static LongWritable one = new LongWritable(1);
        public String whatIsTheKey (char c) {
            if (c >= '0' && c <= '9')
                return "number";
            else if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))
                return ("" + Character.toUpperCase(c));
            else
                return "symbol";
        }
        @Override
        public void run(Context context) throws IOException, InterruptedException {
          setup(context);
          int count = 0;
          while (context.nextKeyValue() && count++ < 1000) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
          }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "_");
            int count = 0;
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                String keyword = whatIsTheKey(str.charAt(0));
                word.set(keyword);
                context.write(word, one);
            }
        }
    }

    public static class FloatSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable result = new LongWritable();
        private Text word = new Text();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            word.set(key);
            result.set(sum);
            context.write(word, result);
        }
    }

    private void percentage(Path path, Configuration conf) throws IOException{
        FileSystem fs = FileSystem.get(conf);
        Path respath = new Path(path,"results");
        FSDataOutputStream out = fs.create(respath);
        String Res[][] = new String[28][2];
        long sum = 0;
        int i = 0;
        for (int l=0; l<3; l++) {
            for (int k = 0; k < 10; k++) {
                if (new String((Integer.toString(l)+Integer.toString(i))).equals("28")) {
                    i = 1;
                    break;
                }
                else {
                    Path file = new Path(path, "part-r-000" + l + k);
                    if (!fs.exists(file))
                        throw new IOException("Output not found! " + k);

                    BufferedReader br = null;
                    // average = total sum / number of elements;
                    try {
                        br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
                        long count = 0;
                        String line;
                        while ((line = br.readLine()) != null) {
                            StringTokenizer st = new StringTokenizer(line);
                            String type = st.nextToken();
                            Res[i][0] = type;
                            String countstr = st.nextToken();
                            Res[i][1] = countstr;
                            i++;
                            count = Long.parseLong(countstr);
                            sum += count;

                        }
                    } finally {
                        if (br != null) {
                            br.close();
                        }
                    }
                }
            }
            if (i == 1)
                break;
        }
        long temp = 0;
        float temp1 = 0;
        try{
            for (int j = 0; j < 28; j++) {
                temp = Long.parseLong(Res[j][1]);
                temp1 = temp/sum;
                out.writeBytes(Res[j][0]+" "+Res[j][1]+" "+temp+" "+Float.toString(temp1)+"%\n");
            }

        }
        catch (IOException e){
            System.out.print("failed to write to file");
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Histogram1000(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Histogram1000 <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Histogramfull");
        job.setJarByClass(Histogram1000.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(FloatSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(28);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        Path outputpath = new Path(otherArgs[otherArgs.length - 1]);
        FileOutputFormat.setOutputPath(job, outputpath);
        boolean result = job.waitForCompletion(true);
        percentage(outputpath,conf);
        return (result ? 0 : 1);
    }
}
