package xyz.ylq.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

/*
 * 从一个hbase 表复制数据到另一个表
 * Mapper中可以只过滤需要的数据
 * reducer 只负责写出
 */
public class hFruitReducer2 extends TableReducer<ImmutableBytesWritable, Put, NullWritable>{

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> value,Context context)
			throws IOException, InterruptedException {
		
		// 遍历写出
		for(Put put : value){
			context.write(NullWritable.get(), put);
		}
	}

}
