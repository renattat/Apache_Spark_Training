package mapper;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Iterator;

public class LineMapper implements FlatMapFunction<Row, String> {

    @Override
    public Iterator<String> call(Row row) throws Exception {
        return Arrays.asList(row.toString().split(" ")).iterator();
    }
}
