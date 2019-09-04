package xyz.ylq.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 从一个hbase 表复制数据到另一个表
 * Mapper中可以只过滤需要的数据
 * 第一个参数是 复制的表名
 * 第二个参数是 写入的表名
 */
public class hbaseTableCopyDriver implements Tool{

	// 定义配置信息
	private Configuration configuration = null;
	
	public void setConf(Configuration conf) {
		configuration = conf;
	}

	public Configuration getConf() {
		return configuration;
	}

	public int run(String[] args) throws Exception {
		
		// 1.获取job对象
		Job job = Job.getInstance(configuration);
		
		// 2.设置主类路径
		job.setJarByClass(hbaseTableCopyDriver.class);
		
		// 3.设置Mapper输出KV类型
		TableMapReduceUtil.initTableMapperJob(args[0], 
				new Scan(),
				hbaseToHbaseMapper.class,
				ImmutableBytesWritable.class,
				Put.class, 
				job);
		
		// 4.设置Reducer输出KV类型
		TableMapReduceUtil.initTableReducerJob(args[1], 
				hbaseTableCopyReducer.class, 
				job);
		
		// 5.提交任务
		boolean result = job.waitForCompletion(true);
		
		return result ? 0:1 ;
	}
	
	public static void main(String[] args) {
		try {
			Configuration configuration = HBaseConfiguration.create();
			// 打jar包在集群上运行，可以把配置删掉
//			configuration.set("hbase.zookeeper.quorum", "gwnet01,gwnet02,gwnet03");
			ToolRunner.run(configuration, new hbaseTableCopyDriver(),args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
