package xyz.ylq.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
/*
 * 从一个hbase 表复制数据到另一个表
 * Mapper中可以只过滤需要的数据
 */
		
public class hFruitMapper2 extends TableMapper<ImmutableBytesWritable, Put>{

	@Override
	protected void map(ImmutableBytesWritable key, Result value,Context context)
			throws IOException, InterruptedException {
		Put put = new Put(key.get());
		
		// 1.获取数据
		for(Cell cell : value.rawCells()){
			// 2.判断当前cell是否为 name 列,过滤只需要的列
			if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
				// 3.给put对象赋值
				put.add(cell);
			}
			if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
				// 3.给put对象赋值
				put.add(cell);
			}
		}
		// 4.写出
		context.write(key, put);
	}

}
