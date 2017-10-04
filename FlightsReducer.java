package Flights_lab3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by heimdall on 25.09.17.
 */
public class FlightsReducer extends Reducer<FlightsWritableComparable, Text, String, String> {
    @Override
    protected void reduce(FlightsWritableComparable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String airportName = "";
        String stats = "";

        int name = 0;
        int count = 0;
        float sum = (float) 0, min = 1000000000, max = 0, currVal = 0;
        for (Text i : values) {
           if (name == 0){
               airportName += i;
               name++;
           }
           else{
               currVal = Float.parseFloat(String.valueOf(i));

               if (currVal > max) {
                   max = currVal;
               }
               if (currVal < min) {
                   min = currVal;
               }
               sum += currVal;
               count++;
           }
        }
        if (count > 0){
            stats += "Min:" + min;
            stats += ", Max:" + max;
            stats += ", Avg:" + (sum / (float)count);

            context.write(airportName, stats);
        }
    }
}
