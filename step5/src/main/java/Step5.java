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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Step5 { // write the the collections for each decade in descending order of npmi

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        // input: key = line number, value = line- decade w1 w2 \t npmi relNpmi
        // check that npmi and relNpmi are greater than the minimum values, if so, output the key and value
        // output: key= decade w1 w2 npmi, value= " " for each input line
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t"); //key= decade w1 w2, value= npmi relNpmi
            String[] key_ = data[0].split(" ");
            String decade = key_[0];
            String word1 = key_[1];
            String word2 = key_[2];
            String[] value_ = data[1].split(" ");
            double npmi = Double.parseDouble(value_[0]);
            double relNpmi = Double.parseDouble(value_[1]);
            double minPmi =Double.parseDouble(context.getConfiguration().get("minPmi"));
            double relMinPmi =Double.parseDouble(context.getConfiguration().get("relMinPmi"));
            if(npmi>=minPmi || relNpmi>=relMinPmi){ 
                context.write(new Text(decade + " " + word1 + " " + word2+ " "+ value_[0]), new Text(" ")); // key= decade w2 w1 npmi, value= " "
            }
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


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        // input: key= decade w1 w2 npmi, value= " "
        // the inputs are sorted in descending order of npmi, so just write the key and value
        // output: key= decade w1 w2, value= npmi
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringTokenizer keyIterator = new StringTokenizer(key.toString());
            String decade = keyIterator.nextToken();
            String word1 = keyIterator.nextToken();
            String word2 = keyIterator.nextToken();
            String npmi = keyIterator.nextToken();
            context.write(new Text(decade + " " + word1 + " " + word2), new Text(npmi)); // key= decade w1 w2, value= npmi
        }
    }

    public static class DescendingNPMIComparator extends WritableComparator {
        protected DescendingNPMIComparator() {
            super(Text.class, true);
        }

        // Sort by decade, then by npmi in descending order
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String[] key1 = w1.toString().split(" "); //decade w1 w2 npmi
            String[] key2 = w2.toString().split(" "); //decade' w1' w2' npmi'
            int decade1 = Integer.parseInt(key1[0]);
            int decade2 = Integer.parseInt(key2[0]);
            double npmi1 = Double.parseDouble(key1[3]);
            double npmi2 = Double.parseDouble(key2[3]);
            // Compare decades first
            int decadeComparison = Integer.compare(decade1, decade2);
            if (decadeComparison != 0) {
                return decadeComparison;
            }
            // If decades are equal, compare npmi in descending order
            return Double.compare(npmi2, npmi1);
        }
    }


    public static void main(String[] args) throws Exception {
        // arg0= inputPath, arg1= outputPath, arg2= minPmi, arg3= relMinPmi
        System.out.println("step5 started");
        Configuration conf = new Configuration();
        conf.set("minPmi", args[2]);
        conf.set("relMinPmi", args[3]);
        //conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes
        Job job = Job.getInstance(conf, "step5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setSortComparatorClass(DescendingNPMIComparator.class);
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