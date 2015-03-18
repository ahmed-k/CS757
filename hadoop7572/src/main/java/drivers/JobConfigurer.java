package drivers;

import compositekeys.PairKey;
import mappers.PairMapper;
import mappers.PairMapperBigDataSet;
import mappers.RelativeFrequencyMapper;
import mappers.StripeMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import partitioners.RelativeFrequencyPartitioner;
import reducers.PairReducer;
import reducers.RelativeFrequencyReducer;
import reducers.StripeReducer;

/**
 * Created by Ahmed Alabdullah on 3/17/15.
 */
public class JobConfigurer {

    private static final String _10K = "10K";
    private static final String _1M = "1M";
    private static final String _10M = "10M";
    private static final String PAIR = "pair";
    private static final String STRIPE = "stripe";
    private static final String RELATIVE_FREQUENCY = "rfreq";


    public static void configureJob(String technique, String dataset, Job job) {

        job.setJarByClass(AssignmentDriver.class);

        if (PAIR.equalsIgnoreCase(technique)) {
           configureForPairs(_10K.equalsIgnoreCase(dataset), job);
        }
        else if (STRIPE.equalsIgnoreCase(technique)){
            configureForStripes(job);
        }
        else if (RELATIVE_FREQUENCY.equalsIgnoreCase(technique)) {
           configureForRelativeFrequency(job);
        }

        job.setInputFormatClass(KeyValueTextInputFormat.class);

    }


    public static void configureForRelativeFrequency(Job job) {

        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setCombinerClass(PairReducer.class);
        job.setPartitionerClass(RelativeFrequencyPartitioner.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(PairKey.class);
        job.setOutputValueClass(IntWritable.class);

    }


    public static void configureForStripes(Job job) {
        job.setMapperClass(StripeMapper.class);
        job.setReducerClass(StripeReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }


    public static void configureForPairs(boolean bigDataSet, Job job) {

        if (bigDataSet) {
            job.setMapperClass(PairMapperBigDataSet.class);
        }
        else {
            job.setMapperClass(PairMapper.class);
        }

        job.setCombinerClass(PairReducer.class);
        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(PairKey.class);
        job.setOutputValueClass(IntWritable.class);

    }


}
