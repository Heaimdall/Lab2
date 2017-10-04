package Flights_lab3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Created by heimdall on 25.09.17.
 */
public class FlightsPartitioner extends Partitioner<FlightsWritableComparable, Text> {

    @Override
    public int getPartition(FlightsWritableComparable flightsWritableComparable, Text text, int i) {
        return flightsWritableComparable.getDestAeroportId() % i;
    }
}
