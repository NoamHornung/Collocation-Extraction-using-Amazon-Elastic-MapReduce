import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step4 { //calculate the relative npmi for each 2-gram

    public static class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        // input: key = line number, value = line- decade w1 w2 \t npmi
        // output: key= decade * *, value= npmi and key= decade w1 w2, value= npmi for each input line
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t"); //key= decade w1 w2, value= npmi
            String[] key_ = data[0].split(" ");
            String decade = key_[0];
            String word1 = key_[1];
            String word2 = key_[2];
            double npmi = Double.parseDouble(data[1]);
            context.write(new Text(decade + " * *"), new DoubleWritable(npmi)); // key= decade * *, value= npmi
            context.write(new Text(decade + " " + word1 + " " + word2), new DoubleWritable(npmi)); // key= decade w2 w1, value= npmi
        }
    }

    // partition by decade
    public static class PartitionerClass extends Partitioner<Text, DoubleWritable> {
        @Override
        public int getPartition(Text key, DoubleWritable value, int numPartitions) {
            String[] data = key.toString().split(" ");
            String decade= data[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    public static class combinerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        // combine the npmi values of the keys with * *, otherwise just write the value (only one value for each key)
        @Override 
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String[] key_ = key.toString().split(" "); //decade * * or decade w1 w2
            String word1= key_[1];
            String word2= key_[2];
            if(word1.equals("*") && word2.equals("*")){
                double sum = 0;
                for (DoubleWritable value : values) {
                    sum += value.get();
                }
                context.write(key, new DoubleWritable(sum));
            }
            else{
                DoubleWritable value = values.iterator().next();
                context.write(key, value);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, DoubleWritable, Text, Text> {
        private double decadeNpmiCount;
        
        @Override
        protected void setup(Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            decadeNpmiCount = 0;
        }

        // input: key= decade w1 w2, value= npmi or key= decade * *, value= npmi
        // for decade * * sum the npmi values
        // for decade w1 w2 calculate relNpmi= npmi/decadeNpmiCount and write the npmi and relNpmi values
        // output: key= decade w1 w2, value= npmi relNpmi

        @Override
        public  void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            StringTokenizer keyIterator = new StringTokenizer(key.toString());
            String decade = keyIterator.nextToken();
            String word2 = keyIterator.nextToken();
            String word1 = keyIterator.nextToken();
            if(word1.equals("*") && word2.equals("*")){
                decadeNpmiCount = 0;
                for (DoubleWritable value : values) {
                    decadeNpmiCount += value.get();
                }
            }
            else{ // key= decade w1 w2, value= npmi
                double npmi = values.iterator().next().get();
                double relNpmi= npmi/decadeNpmiCount;
                context.write(key, new Text(String.valueOf(npmi)+" "+ String.valueOf(relNpmi))); // key= decade w1 w2, value= npmi relNpmi
            }
            
        }
    }


    public static void main(String[] args) throws Exception {
        // arg0= inputPath, arg1= outputPath
        System.out.println("step4 started");
        Configuration conf = new Configuration();
        //conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes
        Job job = Job.getInstance(conf, "step4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(combinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}