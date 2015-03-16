package drivers;

import compositekeys.PairKey;
import mappers.PairMapper;
import mappers.PairMapperBigDataSet;
import mappers.StripeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import reducers.PairReducer;
import reducers.StripeReducer;

/**
 * Created by Ahmed Alabdullah on 3/15/15.
 */
public class JobDriver {

    private static final String _10K = "10K";
    private static final String _1M = "1M";
    private static final String _10M = "10M";
    private static final String PAIR = "pair";
    private static final String STRIPE = "stripe";



    private static void configureStripes(Job job) {
        job.setMapperClass(StripeMapper.class);
        job.setCombinerClass(StripeReducer.class);
        job.setReducerClass(StripeReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }


    private static void configurePairs(boolean bigDataSet, Job job) {

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


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        if (otherArgs.length != 4) {
            System.err.println("Usage: JobDriver <in> <out> <pair|stripe> <dataset>");
            System.exit(2);
        }

        String technique = otherArgs[2];
        String dataset = otherArgs[3];

        //CONFIGURE THE JOB
        Job job = new Job(conf, "movie pairs");
        job.setJarByClass(JobDriver.class);

        if (PAIR.equalsIgnoreCase(technique)) {
            configurePairs(_10K.equalsIgnoreCase(dataset), job);
        }
        else {
            configureStripes(job);
        }


        //map-reduce classes
        job.setCombinerClass(PairReducer.class);
        job.setReducerClass(PairReducer.class);

        //key-val classes
        job.setOutputKeyClass(PairKey.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0 :1);

    }


}
