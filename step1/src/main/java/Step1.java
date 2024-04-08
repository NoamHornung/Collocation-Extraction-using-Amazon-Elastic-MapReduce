import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step1 { // calculate c(w1,w2) of each decade for the valid 2-grams and c(w1) for each decade

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        final String[] englishWordsArray = {
            "a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", 
            "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", 
            "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", 
            "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", 
            "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", 
            "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", 
            "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", 
            "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", 
            "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", 
            "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", 
            "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", 
            "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", 
            "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", 
            "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", 
            "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", 
            "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", 
            "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", 
            "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", 
            "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", 
            "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", 
            "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", 
            "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", 
            "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", 
            "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", 
            "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", 
            "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", 
            "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", 
            "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", 
            "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", 
            "yourselves"
        };

        final String[] hebrewWordsArray = {
            "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ",
            "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר",
            "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה",
            "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר",
            "אם", "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":",
            "1", ".", "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם",
            "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי",
            "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר",
            "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל",
            "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"
        };

        HashMap<String, Integer> stopWords = new HashMap<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            String language =context.getConfiguration().get("language");
            if(language.equals("english")){
                for (String word : englishWordsArray) {
                    stopWords.put(word, 1);
                }
            }
            else{
                for (String word : hebrewWordsArray) {
                    stopWords.put(word, 1);
                }
            }
        }

        // key= line number, value= the line itself = 2-gram \t year \t num of occurrence \t (irrelevant)
        // output: key= decade w1 *, value= occurrence and key= decade w1 w2, value= occurrence for each input line
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //randomly skip 50% of the lines
            //TODO remove later
            // double randomNumber = Math.random();
            // if(randomNumber > 0.5){
            //     return;
            // }
            String line = value.toString();
            //System.out.println("line: " + line);
            String[] data = line.split("\t");
            // valid input should be: [ 2-gram=(w1 w2), year, num of occurrence, (irrelevant), (irrelevant)]
            if(data.length < 4){ // is it 5?
                System.out.println("invalid input: less then 4 parameters" + line);
                return;
            }
            String[] words = data[0].split(" ");
            if(words.length != 2){
                System.out.println("invalid input: not 2 words " + line);
                return;
            }
            String word1 = words[0];
            String word2 = words[1];
            String language =context.getConfiguration().get("language");
            if(language.equals("english")){
                String word1Lower= word1.toLowerCase();
                String word2Lower= word2.toLowerCase();
                if(stopWords.containsKey(word1Lower) || stopWords.containsKey(word2Lower) || !word1Lower.matches("^[a-z]*$") || !word2Lower.matches("^[a-z]*$")){ //only english lowercase letters  
                    //System.out.println("contains stop word or not only letters");
                    return;
                }
            }
            else{
                if(stopWords.containsKey(word1) || stopWords.containsKey(word2) || !word1.matches("^[\\u05D0-\\u05EA]*$") || !word2.matches("^[\\u05D0-\\u05EA]*$")){ //only hebrew letters
                    //System.out.println("contains stop word");    
                    return;
                }
            }
            
            int year = Integer.parseInt(data[1]);
            String decade = String.valueOf(year - (year % 10));
            String occurrence = data[2];
            //System.out.println(decade+ " " + word1 + " " + word2 + " " + occurrence);
            context.write(new Text(decade + " " + word1 + " *"), new LongWritable(Long.parseLong(occurrence))); // key= decade w1 *, value= occurrence
            context.write(new Text(decade + " " + word1 + " " + word2), new LongWritable(Long.parseLong(occurrence))); // key= decade w1 w2, value= occurrence
        }
        
    }

    // partition by the decade
    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String[] data = key.toString().split(" ");
            String decade= data[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    public static class combinerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        // combine the occurrence values of the same key 
        @Override 
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum)); // key= decade w1 w2, value= c(w1,w2)
        }
    }

    // sum the values of the same key
    // key= decade w1 w2, value= list<occurrence>
    public static class ReducerClass extends Reducer<Text, LongWritable, Text, Text> {
        private long firstWordCount;
        
        @Override
        protected void setup(Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            firstWordCount = 0;
        }

        // input: key= decade w1 w2, value= occurrence or key= decade w1 *, value= occurrence
        // for decade w1 * sum the occurrence values- this is c(w1)
        // for decade w1 w2 write sum the occurrence values- this is c(w1,w2)
        // output: key= decade w1 w2, value= c(w1,w2) c(w1)
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            StringTokenizer keyIterator = new StringTokenizer(key.toString());
            String decade = keyIterator.nextToken();
            String word1 = keyIterator.nextToken();
            String word2 = keyIterator.nextToken();
            if(word2.equals("*")){
                firstWordCount = 0;
                for (LongWritable value : values) {
                    firstWordCount += value.get();
                }
            }
            else{
                long c_w1_w2 = 0;
                for (LongWritable value : values) {
                    c_w1_w2 += value.get();
                }
                context.write(key, new Text(String.valueOf(c_w1_w2)+ " " + String.valueOf(firstWordCount))); // key= decade w1 w2, value= c(w1,w2) c(w1)
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // arg0= inputPath, arg1= outputPath, arg2= language
        System.out.println("step1 started");
        Configuration conf = new Configuration();
        conf.set("language", args[2]);
        //conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes
        Job job = Job.getInstance(conf, "step1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(combinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}