package drivers;

import customkeys.MatrixVectorWritable;
import customkeys.MatrixWritable;
import mapreducers.BatchGradientDescent;
import mapreducers.IterationController;
import mapreducers.MatrixMultiplier;
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
        String m = args[2];
        String n = args[3];
        String d = args[4];
        Configuration conf = new Configuration();
        conf.set("m", m);
        conf.set("n", n);
        conf.set("d", d);

        //prepare job config object
        //create job from config object
        Job job;
        if (args.length == 6) {
           job = configureMultiplyJob(conf);
        }
        else {
           job = configureBGDJob(conf);
        }
        //set input and output paths then exit
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
/*
        if(job.waitForCompletion(true)) {

            Job job2 = createIterationJob();
            if(job2.waitForCompletion(true)) {
*/
/*                //Job job3 = createCollapseJob();
                if (job3.waitForCompletion(true)) {

                }*//*


            }


        }
*/

        System.exit(job.waitForCompletion(true)? 0 :1);
    }

    public static Job configureMultiplyJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "matrix multiplication");
        job.setJarByClass(AssignmentDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(MatrixMultiplier.MultiplierMapper.class);
        job.setReducerClass(MatrixMultiplier.MultiplierReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixVectorWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static Job createIterationJob() throws IOException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "iteration determination");
        job.setJarByClass(AssignmentDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(IterationController.IterationMapper.class);
        job.setReducerClass(IterationController.IterationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static Job configureBGDJob(Configuration conf ) throws IOException {
        Job job = new Job(conf, "batch gradient descent");
        job.setJarByClass(AssignmentDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(BatchGradientDescent.BGDMapper.class);
        job.setReducerClass(BatchGradientDescent.BGDReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }


}
