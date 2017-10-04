package Flights_lab3;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by heimdall on 25.09.17.
 */
public class FlightsComparator extends WritableComparator {
    public  FlightsComparator(){super(FlightsWritableComparable.class, true);}
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return Integer.compare(((FlightsWritableComparable) a).getDestAeroportId(), ((FlightsWritableComparable) b).getDestAeroportId());
    }
}
