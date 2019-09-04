package xyz.ylq.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/*
 * reducer
 * 把hdfs文件系统上的文件加载到hbase表中
 * 可以动态设置 列族和列名
 */
public class hFruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
	String cf = null;
	
	// 这个方法可以设置全局的 属性
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		
		configuration.set("cf", "info");
	}


	@Override
	protected void reduce(LongWritable key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
		
		// 循环每一条数据
		for (Text text : value) {
			// 获取每一行数据
			String[] fields = text.toString().split("\t");
			
			Put put = new Put(Bytes.toBytes(fields[0]));
			// put 对象赋值
			// 列族、列可以动态获取
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));
			// 写出
			context.write(NullWritable.get(), put);
		}
	}
	
}
