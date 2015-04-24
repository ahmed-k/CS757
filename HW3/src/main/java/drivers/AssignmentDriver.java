package drivers;

import mappers.BatchGradientDescent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by alabdullahwi on 4/21/2015.
 */
public class AssignmentDriver {

    public static void main(String[] args) throws Exception {
        //get args
        String UDim = args[2];
        String VDim = args[3];
        String intDim = args[4];
        //prepare job config object
        Configuration conf = new Configuration();
        conf.set("UDim", UDim);
        conf.set("VDim", VDim);
        conf.set("intDim", intDim);
        //create job from config object
        Job job = configureJob(conf);
        //set input and output paths then exit
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0 :1);
    }

    public static Job configureJob(Configuration conf ) throws IOException {
        Job job = new Job(conf, "batch gradient descent");
        job.setJarByClass(AssignmentDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(BatchGradientDescent.BGDMapper.class);
        job.setReducerClass(BatchGradientDescent.BGDReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }


}
