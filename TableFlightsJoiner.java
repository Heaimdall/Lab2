package Flights_lab3;

/**
 * Created by heimdall on 25.09.17.
 */

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


public class TableFlightsJoiner {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TableFlightsJoiner <input path flight> <input path airport> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();

        job.setJarByClass(TableFlightsJoiner.class);
        job.setJobName("Flights (hadooplab3)");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setPartitionerClass(FlightsPartitioner.class);
        job.setGroupingComparatorClass(FlightsComparator.class);
        job.setReducerClass(FlightsReducer.class);
        job.setMapOutputKeyClass(FlightsWritableComparable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(FlightsWritableComparable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}