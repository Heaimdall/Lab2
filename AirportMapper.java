package Flights_lab3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by heimdall on 25.09.17.
 */
public class AirportMapper extends Mapper<LongWritable, Text, FlightsWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] column = value.toString().replace("\"", "").split(",(?! )");

        String code = column[0];
        String name = column[1];
        Integer flag = 0;

        if (!code.equals("Code")){

            FlightsWritableComparable entryPair = new FlightsWritableComparable();
            entryPair.setFlag(flag);
            entryPair.setDestAeroportId(Integer.parseInt(code));

            Text AirportName = new Text(name);

            context.write(entryPair, AirportName);

        }

    }
}
