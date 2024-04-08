import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 { //calculate c(w2)

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        // input: key = line number, value = line- decade w1 w2 \t c(w1,w2) c(w1)
        // output: key= decade w2 *, value= c(w1,w2) and key= decade w2 w1, value= c(w1,w2) c(w1) for each input line
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t"); //key, value
            String[] key_ = data[0].split(" ");
            String decade = key_[0];
            String word1 = key_[1];
            String word2 = key_[2];
            String[] value_ = data[1].split(" ");
            String c_w1_w2 = value_[0];
            context.write(new Text(decade + " " + word2 + " *"), new Text(c_w1_w2)); // key= decade w2 *, value= c(w1,w2)
            context.write(new Text(decade + " " + word2 + " " + word1), new Text(data[1])); // key= decade w2 w1, value= c(w1,w2) c(w1)
        }
    }

    // partition by decade
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] data = key.toString().split(" ");
            String decade= data[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    public static class combinerClass extends Reducer<Text, Text, Text, Text> {
        // combine the c(w1,w2) values of the keys with *, otherwise just write the value (only one value for each key)
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] key_ = key.toString().split(" "); //decade w2 * or decade w2 w1
            String word1= key_[2];
            if(word1.equals("*")){
                long sum = 0;
                for (Text value : values) {
                    sum += Long.parseLong(value.toString());
                }
                context.write(key, new Text(String.valueOf(sum))); // key= decade w2 *, value= c(w2,*)
            }
            else{
                Text value = values.iterator().next();
                context.write(key, value); // key= decade w2 w1, value= c(w1,w2) c(w1)
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private long secondWordCount;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            secondWordCount = 0;
        }

        // input: key= decade w2 * value = c(w1,w2) or key= decade w2 w1 value= c(w1,w2) c(w1)
        // for decade w2 * sum the c(w1,w2) values- this is c(w2)
        // for decade w2 w1 write the c(w1,w2), c(w1) and c(w2) values
        // output: key= decade w1 w2, value= c(w1,w2) c(w1) c(w2)
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringTokenizer keyIterator = new StringTokenizer(key.toString());
            String decade = keyIterator.nextToken();
            String word2 = keyIterator.nextToken(); // word2, first in the pair
            String word1 = keyIterator.nextToken(); // word1, second in the pair
            if(word1.equals("*")){
                secondWordCount = 0;
                for (Text value : values) {
                    secondWordCount += Long.parseLong(value.toString());
                }
            }
            else{
                String value = values.iterator().next().toString();
                String c_w2= String.valueOf(secondWordCount);
                context.write(new Text(decade + " " + word1 + " " + word2), new Text(value + " " + c_w2)); // key= decade w1 w2, value= c(w1,w2) c(w1) c(w2)
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // arg0= inputPath, arg1= outputPath
        System.out.println("step2 started");
        Configuration conf = new Configuration();
        //conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes
        Job job = Job.getInstance(conf, "step2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(combinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}