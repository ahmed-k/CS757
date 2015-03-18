package drivers;

import compositekeys.PairKey;
import mappers.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import partitioners.PairPartitioner;
import reducers.LiftReducer;
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
    private static final String LIFT = "lift";
    private static final String SON = "son";




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
        else if (LIFT.equalsIgnoreCase(technique)) {
            configureForLift(job);
        }

        else if (SON.equalsIgnoreCase(technique)) {
            configureForSON(job);
        }

        job.setInputFormatClass(KeyValueTextInputFormat.class);

    }


    public static void configureForSON(Job job) {

        job.setMapperClass(FrequentItemsetMapper.class);
        job.setReducerClass(PairReducer.class);

        job.setOutputKeyClass(PairKey.class);
        job.setOutputValueClass(IntWritable.class);


    }

    public static void configureForRelativeFrequency(Job job) {

        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setCombinerClass(PairReducer.class);
        job.setPartitionerClass(PairPartitioner.class);

        job.setMapOutputKeyClass(PairKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairKey.class);
        job.setOutputValueClass(DoubleWritable.class);

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

    public static void configureForLift(Job job) {

        job.setMapperClass(LiftMapper.class);
        job.setReducerClass(LiftReducer.class);
        job.setCombinerClass(PairReducer.class);
        job.setPartitionerClass(PairPartitioner.class);

        job.setMapOutputKeyClass(PairKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairKey.class);
        job.setOutputValueClass(DoubleWritable.class);

    }


}
