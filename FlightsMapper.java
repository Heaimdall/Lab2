package Flights_lab3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.concurrent.Delayed;

/**
 * Created by heimdall on 22.09.17.
 */
public class FlightsMapper  extends Mapper<LongWritable, Text, FlightsWritableComparable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] column = value.toString().split(",");

        String Year = column[0];
        String Cancelled = column[19];
        String Delayed = column[18];
        String AiroportId = column[14];
        String empty = "";
        Integer flag = 1;

        if ((!Year.equals("\"YEAR\""))
                && (!Cancelled.equals(empty))
                && ((float) 0 == Float.parseFloat(Cancelled))
                && (!Delayed.equals(empty) && ((float)0 < Float.parseFloat(Delayed)))
                && (!AiroportId.equals(empty))){

            FlightsWritableComparable entryPair = new FlightsWritableComparable();
            entryPair.setFlag(flag);
            entryPair.setDestAeroportId(Integer.parseInt(AiroportId));

            Text delay_message = new Text(Delayed);

            context.write(entryPair, delay_message);
        }





    }

    
}
