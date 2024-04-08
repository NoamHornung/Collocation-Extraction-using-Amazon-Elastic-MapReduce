import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3 { //calculate N- the number of the 2-gram in each decade and the npmi for each 2-gram

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        // input: key = line number, value = line- decade w1 w2 \t c(w1,w2) c(w1) c(w2)
        // output: key= decade * *, value= c(w1,w2) and key= decade w1 w2, value= c(w1,w2) c(w1) c(w2) for each input line
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t"); //key, value
            String[] key_ = data[0].split(" ");
            String decade = key_[0];
            String word1 = key_[1];
            String word2 = key_[2];
            String[] value_ = data[1].split(" "); //c(w1,w2) c(w1) c(w2)
            String c_w1_w2 = value_[0];
            //System.out.println("value: " +data[1]);
            context.write(new Text(decade + " * *"), new Text(c_w1_w2)); // key= decade * *, value= c(w1,w2)
            context.write(new Text(decade + " " + word1 + " " + word2), new Text(data[1])); // key= decade w2 w1, value= c(w1,w2) c(w1) c(w2)
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
        // combine the c(w1,w2) values of the keys with * *, otherwise just write the value (only one value for each key)
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] key_ = key.toString().split(" "); //decade * * or decade w1 w2
            String word1= key_[1];
            String word2= key_[2];
            if(word1.equals("*") && word2.equals("*")){
                long sum = 0;
                for (Text value : values) {
                    sum += Long.parseLong(value.toString());
                }
                //System.out.println("[combiner] sum: "+ sum);
                context.write(key, new Text(String.valueOf(sum)));
            }
            else{
                Text value = values.iterator().next();
                //System.out.println("[combiner] key: "+ key+ " value: "+ value);
                context.write(key, value);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
        private long decadeCount;
        
        @Override
        protected void setup(Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            decadeCount = 0;
        }

        
        // input: key= decade * * value = c(w1,w2) or key= decade w1 w2 value= c(w1,w2) c(w1) c(w2)
        // for decade * * sum the c(w1,w2) values- this is N
        // for decade w1 w2 calculate the npmi value using c(w1,w2), c(w1), c(w2) and N
        // output: key= decade w1 w2, value= npmi
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringTokenizer keyIterator = new StringTokenizer(key.toString());
            String decade = keyIterator.nextToken();
            String word1 = keyIterator.nextToken();
            String word2 = keyIterator.nextToken();
            if(word1.equals("*") && word2.equals("*")){
                decadeCount = 0;
                for (Text value : values) {
                    decadeCount += Long.parseLong(value.toString());
                }
                //System.out.println("decade: "+ decade+ "decadeCount: " + decadeCount);
            }
            else{ // key= decade w1 w2, value= c(w1,w2) c(w1) c(w2)
                String value = values.iterator().next().toString();
                String[] value_ = value.split(" ");
                long N = decadeCount;
                long c_w1_w2 = Long.parseLong(value_[0]);
                long c_w1 = Long.parseLong(value_[1]);
                long c_w2 = Long.parseLong(value_[2]);
                //System.out.println("c_w1_w2: "+ c_w1_w2+ " c_w1: " + c_w1+ " c_w2: " + c_w2+ " N: " + N);
                double numerator = Math.log(c_w1_w2)+ Math.log(N)- Math.log(c_w1)- Math.log(c_w2);
                double denominator = -(Math.log(c_w1_w2)-Math.log(N));
                //System.out.println("numerator: "+ numerator+ " denominator: " + denominator);
                double npmi = numerator/denominator;
                //System.out.println(key+ " "+ npmi);
                context.write(key, new DoubleWritable(npmi)); // key= decade w1 w2, value= npmi
            }
            
        }
    }


    public static void main(String[] args) throws Exception {
        // arg0= inputPath, arg1= outputPath
        System.out.println("step3 started");
        Configuration conf = new Configuration();
        //conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes
        Job job = Job.getInstance(conf, "step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(combinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}