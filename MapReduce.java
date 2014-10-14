import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 *  Yang Liu, Fall 2013
 */

/*
 *  Finds the co-occurrence rate between a target word and every other unique word in a given text document
 */

/*
 *  DoublePair is a custom Writeable class which holds two double variables. Func is an custom abstract class used to
 *  represent a function which takes in a double value and returns a double value. These two classes are defined separately outside this file.
 */

public class MapReduceProject{

    /*
     * Inputs is a set of (docID, document contents) pairs.
     */
    public static class Map1 extends Mapper<WritableComparable, Text, Text, DoublePair> {
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");

        private String targetGram = null;
        private int funcNum = 0;

        @Override
            public void setup(Context context) {
                targetGram = context.getConfiguration().get("targetWord").toLowerCase();
                try {
                    funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
                } catch (NumberFormatException e) {
                    /* Do nothing. */
                }
            }

        @Override
            public void map(WritableComparable docID, Text docContents, Context context)
            throws IOException, InterruptedException {
                Matcher matcher = WORD_PATTERN.matcher(docContents.toString());
                Func func = funcFromNum(funcNum);
		
                
	            boolean first_target = true;
                ArrayList<String> word_counter = new ArrayList<String>(); //stores all non-target words that comes before a specific instance of a target word
                int distance = 0;
	            while (matcher.find()) {

		          String word = matcher.group().toLowerCase();
		          if (!word.equals(targetGram)){
			         word_counter.add(word);
		          } 

                  else {
                        //Calculates for first instance of target word.
                        if (first_target = true){ 

                            for (int i = word_counter.size() - 1; i >= 0; i--){
                                distance++;
                                Text output = new Text(word_counter.get(i));
                                DoublePair output_pair = new DoublePair (func.f(distance), 1);
                                context.write(output, output_pair);
                            }

                            first_target = false;

                        } 

                        //Calculates distance for other instances of target word.
                        //We need to do two iterations to properly calculate the distance between a non-target word that's between two target word instances.    
                        else { 

                            //First half of distance calculation
                            for (int j = 0; j < (word_counter.size() / 2); j++){ 
                                distance++;
                                Text output = new Text(word_counter.get(j));
                                DoublePair output_pair = new DoublePair (func.f(distance), 1);
                                context.write(output, output_pair);
                            }

                            distance = 0;
                            //Second half
                            for (int k = word_counter.size() - 1; k >= (word_counter.size() / 2); k--) { 
                                distance++;
                                Text output = new Text(word_counter.get(k));
                                DoublePair output_pair = new DoublePair (func.f(distance), 1);
                                context.write(output, output_pair);
                            }
                        }
                    word_counter = new ArrayList<String>(); //reset the counter for the next target word instance
                    distance = 0; //resets array and distance
		        }
            } //end while loop for matcher.find

            if (first_target == true) { //checks for cases where no target word is found
                for (int l = 0; l < word_counter.size(); l++){
                    Text output = new Text(word_counter.get(l));
                    DoublePair output_pair = new DoublePair (func.f(Double.POSITIVE_INFINITY), 1);
                    context.write(output, output_pair);
                }
            } else {
                for (int m = 0; m < word_counter.size(); m++){
                    distance++;
                    Text output = new Text(word_counter.get(m));
                    DoublePair output_pair = new DoublePair (func.f(distance), 1);
                    context.write(output, output_pair);
                }
            }
        }
        //end map

        /** Returns the Func corresponding to FUNCNUM*/
        private Func funcFromNum(int funcNum) {
            Func func = null;
            switch (funcNum) {
                case 0:	
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
                        }			
                    };	
                    break;
                case 1:
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
                        }			
                    };
                    break;
                case 2:
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + Math.sqrt(d);
                        }			
                    };
                    break;
            }
            return func;
        }
    }

    public static class Combine1 extends Reducer<Text, DoublePair, Text, DoublePair> {

        @Override
            public void reduce(Text key, Iterable<DoublePair> values,
                    Context context) throws IOException, InterruptedException {

                double sum_x = 0;
                double sum_y = 0;
                for (DoublePair val : values){
                    sum_x = sum_x + val.x;
                    sum_y = sum_y + val.y;
                }
                DoublePair output_pair = new DoublePair(sum_x, sum_y);
                context.write(key, output_pair);
            }
    }


    public static class Reduce1 extends Reducer<Text, DoublePair, DoubleWritable, Text> {
        @Override
            public void reduce(Text key, Iterable<DoublePair> values,
                    Context context) throws IOException, InterruptedException {

                double sum_x = 0;
                double sum_y = 0;
                double rate = 0;
                for (DoublePair val : values){
                    sum_x = sum_x + val.getDouble1();
                    sum_y = sum_y + val.getDouble2();
                }
                if (sum_x > 0){
                    rate = (((sum_x * (Math.pow(Math.log(sum_x), 3))) / sum_y) * -155) + 50;
                }
                context.write(new DoubleWritable(rate), key);
            }
    }

    public static class Map2 extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
    
    }

    public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        int n = 0;
        static int N_TO_OUTPUT = 110;
      
        @Override
            protected void setup(Context c) {
                n = 0;
            }

        @Override
            public void reduce(DoubleWritable key, Iterable<Text> values,
                    Context context) throws IOException, InterruptedException {

                for (Text output : values){
                    double rate = key.get();
                    rate = Math.abs(rate);
                    DoubleWritable wrappedRate = new DoubleWritable(rate);
                    context.write(wrappedRate, output);
                    n++;
                    if (n >= N_TO_OUTPUT){
                        break;
                }
            }
        }
    }
    
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", false);

        System.out.println("Target word: " + conf.get("targetWord"));
        System.out.println("Function num: " + conf.get("funcNum"));

        if(runJob2)
            System.out.println("running both jobs");
        else
            System.out.println("for debugging, only running job 1");

        if(combiner)
            System.out.println("using combiner");
        else
            System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
            System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
            System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
            System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
            System.exit(1);
        }

        {
            Job firstJob = new Job(conf, "job1");

            firstJob.setJarByClass(Map1.class);

            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(DoublePair.class);
            firstJob.setOutputKeyClass(DoubleWritable.class);
            firstJob.setOutputValueClass(Text.class);

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
                firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
                firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);

            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "job2");

            secondJob.setJarByClass(Map1.class);
            
            secondJob.setMapOutputKeyClass(DoubleWritable.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);

            secondJob.setMapperClass(Map2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }
}

