This assignment involves automatically extracting collocations from the Google 2-grams dataset using Amazon Elastic MapReduce (EMR). Collocations are sequences of words or terms that co-occur more frequently than expected by chance, and their identification is crucial for various natural language processing and information extraction tasks.

Key Components:
- Collocation Criteria:
The assignment utilizes Normalized Pointwise Mutual Information (NPMI) to determine whether a given pair of ordered words is a collocation.
Collocation determination is based on:
Minimal PMI: If the NPMI value is equal to or greater than a specified minimal PMI value.
Relative Minimal PMI: If the NPMI value divided by the sum of all NPMI values in the same decade (including those below the minimal PMI) is equal to or greater than a specified relative minimal PMI value.

- Map-Reduce Job:
The map-reduce job is designed to extract collocations for each decade.
Parameters such as minPmi and relMinPmi values are passed to the job.
The job calculates the normalized PMI of each pair of words in each decade and displays all collocations above each of the two minimum inputs.


# Instructions to run the project:
1. Configure your AWS credentials by adding them to the ~user/.aws/credentials file.
2. Create an AWS S3 bucket and update the bucket name in main/src/main/java/Main.java to match your created bucket name.
3. Build the JAR files for the main project and its steps:
    -Navigate to the root directory of each project.
    -Execute the command 'mvn clean package'.
4. Locate the JAR files:
    -Find main-jar-with-dependencies.jar in the "main/target" directory.
    -Find stepX-jar-with-dependencies.jar in the "stepX/target" directory for each step from 1 to 5.
5. Upload the steps JAR files into a "jars" folder within the bucket in S3.
6. Run the main project:
    -Navigate to the location of main-jar-with-dependencies.jar.
    -Execute the command: java -jar main-jar-with-dependencies.jar minNpmi relMinNpmi language
    Replace:
    -minNpmi with a double between 0 and 1 representing the desired minimum npmi value.
    -relMinNpmi with a double between 0 and 1 representing the desired minimum relative npmi value.
    -language with "hebrew" or "english".
7. The output will be available in the "heb_outputs" and "eng_outputs" folders within your S3 bucket accordingly.

# How our program works:
Our program consists of a main class and 5 Hadoop MapReduce steps. It analyzes Hebrew and English 2-Gram datasets from Google Books Ngrams, calculating npmi values and relative npmi values. The output of the program is a list of the collocations for each decade, and there npmi value, ordered by their npmi (descending).
 Here's an overview of each step:
 - Step 1:
    The purpose of this step is to filter out 2-grams containing stop words and calculate the count of ordered pairs (c(w1,w2)) and calculate the count of single words starting a 2-gram (c(w1)).
    - Mapper:
        Input: Key = line number, Value = the line itself (format: 2-gram \t year \t num of occurrence \t irrelevant)
        Output for each input line:
      
            Key = decade w1 *, Value = occurrence
            Key = decade w1 w2, Value = occurrence
      
    - Combiner: (Optional)
        Sum the occurrences for each key.
    - Reducer:
        Input: Key = 'decade w1 w2' or 'decade w1 *', Value = occurrence
      
        For 'decade w1 *', sum the occurrence values = (c(w1)).
      
        For 'decade w1 w2', sum the occurrence values = (c(w1,w2)).
      
        Output: Key = decade w1 w2, Value = c(w1,w2) c(w1)

 - Step 2:
    The purpose of this step is to calculate the number of times each word w2 ends some 2-gram (c(w2)).
    - Mapper:
        Input: key = line number, value = line- decade w1 w2 \t c(w1,w2) c(w1)
        Output for each input line:
      
            key= decade w2 *, value= c(w1,w2).
            key= decade w2 w1, value= c(w1,w2) c(w1).
      
    - Combiner: (Optional)
        Combine the c(w1,w2) values for keys with * as the second word. For other keys, retain the single value.
    - Reducer:
        Input: Key = 'decade w2 *' Value = c(w1,w2) or 'decade w2 w1' Value = c(w1,w2) c(w1)
      
        For 'decade w2 *', sum the c(w1,w2) values = (c(w2)).
      
        For 'decade w2 w1', output c(w1,w2), c(w1), and c(w2) values.
      
        Output: Key = decade w1 w2, Value = c(w1,w2) c(w1) c(w2).

 - Step 3:
    The purpose of this step is to calculate the total number of 2-grams in each decade (N) and compute the normalized pointwise mutual information (npmi) for each 2-gram.
    - Mapper:
        Input: key = line number, value = line- decade w1 w2 \t c(w1,w2) c(w1) c(w2)
        Output for each input line:
      
            key= decade * *, value= c(w1,w2).
            key= decade w1 w2, value= c(w1,w2) c(w1) c(w2).
      
    - Combiner: (Optional)
        Combine the c(w1,w2) values for keys with * * as the words. For other keys, retain the single value.
    - Reducer:
        Input: Key = decade * * or decade w1 w2, Value = c(w1,w2) or c(w1,w2) c(w1) c(w2)
      
        For 'decade * *', sum the c(w1,w2) = N.
      
        For 'decade w1 w2', calculate the npmi value using c(w1,w2), c(w1), c(w2), and N.
      
        Output: Key = decade w1 w2, Value = npmi

- Step 4:
    The purpose of this step is to calculate the relative normalized pointwise mutual information (relNpmi) for each 2-gram.
    - Mapper:
        Input: key = line number, value = line- decade w1 w2 \t npmi
        Output for each input line:
      
            key= decade * *, value= npmi.
            key= key= decade w1 w2, value= npmi.
      
    - Combiner: (Optional)
        Combine the npmi values for keys with * * as the words. For other keys, retain the single value.
    - Reducer:
        Input: key= decade w1 w2, value= npmi or key= decade * *, value= npmi
      
        For for 'decade * *', sum the npmi values.
      
        For 'decade w1 w2', calculate relNpmi = npmi / decadeNpmiCount and output the npmi and relNpmi values.
      
        Output: key= decade w1 w2, value= npmi relNpmi.

- Step 5:
    The purpose of this step is to write the collocations for each decade in descending order of npmi.
    - Mapper:
        Input: key = line number, value = line- decade w1 w2 \t npmi relNpmi.
      
        Check if npmi or relNpmi are greater than the minimum values. If so, output the key and value.
      
        Output for each input line: Key = decade w1 w2 npmi, Value = " ".
      
    - Comparator: Sort the keys by decade, then by npmi in descending order
    - Reducer:
        Input:  Key = decade w1 w2 npmi, Value = " ".
      
        The inputs are already sorted in descending order of npmi, so just rearrange the key and value as needed.
      
        Output: Key = decade w1 w2, Value = npmi.


# Scalability report:
Parameters: minNpmi=0.7, relMinNpmi=0.3.
Instance Type: M4Large.
During processing with 50% of the corpus, the entire corpus is provided as input. However, in the initial mapping stage, a random number is generated for each input record. Only if this randomly generated number is less than 0.5, further processing continues with that specific record.
    
Full run with 100% of the Hebrew corpus with local aggregation took 20 minutes and 53 seconds.

Full run with 100% of the Hebrew corpus without local aggregation took 22 minutes and 41 seconds.

Full run with 50% of the Hebrew corpus with local aggregation took 17 minutes and 38 seconds.

Full run with 100% of the Hebrew corpus with double the number of mappers and local aggregation took 22 minutes and 18 seconds.

Full run with 100% of the English corpus with local aggregation took 4 hours, 34 minutes.

Full run with 50% of the English corpus with local aggregation took 3 hours, 24 minutes.
    
#### with local aggregation on Hebrew corpus:
- Step 1:
  Map input records=252069581
        
  Map output records=232211652
       
  Map output bytes=6774257296
        
  Combine input records=232211652
        
	Combine output records=37770512

  Reduce input records=37770512
  
	Reduce output records=26267782
- Step 2:
  Map input records=26267782
  
	Map output records=52535564

  Map output bytes=1368063555
  
  Combine input records=52535564
  
	Combine output records=28554208

  Reduce input records=28554208
  
	Reduce output records=26267782

- Step 3:
  Map input records=26267782
  
	Map output records=52535564

  Map output bytes=1273947516
  
  Combine input records=52535564
  
	Combine output records=26267823

  Reduce input records=26267823
  
	Reduce output records=26267782

- Step 4:
  Map input records=26267782
  
	Map output records=52535564

  Map output bytes=1318850658
  
  Combine input records=52535564
  
	Combine output records=26267826

  Reduce input records=26267826
  
	Reduce output records=26267782

- Step 5 (without combiner code):
  Map input records=26267782
  
  Map output records=861782
  
  Map output bytes=41801484
  
  Combine input records=0
  
	Combine output records=0

  Reduce input records=861782
  
	Reduce output records=421902

#### without local aggregation on Hebrew corpus:
- Step 1:
  Map input records=252069581
  
	Map output records=232211652

  Map output bytes=6774257296
  
  Reduce input records=232211652
  
	Reduce output records=26267782

- Step 2:
  Map input records=26267782
  
	Map output records=52535564

  Map output bytes=1368063555
  
  Reduce input records=52535564
  
	Reduce output records=26267782

- Step 3:
  Map input records=26267782
  
	Map output records=52535564

  Map output bytes=1273947516
  
  Reduce input records=52535564
  
	Reduce output records=26267782

- Step 4:
  Map input records=26267782
  
	Map output records=52535564

  Map output bytes=1318850658
  
  Reduce input records=52535564
  
	Reduce output records=26267782

- Step 5:
  Map input records=26267782
  
	Map output records=861782

  Map output bytes=41801484
  
  Reduce input records=861782
  
	Reduce output records=421902

(In steps 2, 3, and 4, the mapper produces two output records for each input record, while the reducer generates one output record for every pair of input records.)


