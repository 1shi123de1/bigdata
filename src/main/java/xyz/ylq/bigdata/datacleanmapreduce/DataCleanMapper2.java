package xyz.ylq.bigdata.datacleanmapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import xyz.ylq.bigdata.datacleanmapreduce.DataClean;

public class DataCleanMapper2 extends Mapper<LongWritable, Text, Text ,NullWritable>{

	/*
	 * 用直接用另一个类的静态方法，不成功
	 * 原因未知
	 * 在mapper类下直接写方法的方式是成功的
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String result = DataClean.cleanData(value.toString());
		context.write(new Text(result),NullWritable.get());
	}
}
