package xyz.ylq.bigdata.datacleanmapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataCleanMapper extends Mapper<LongWritable, Text, Text ,NullWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//String result = DataClean.cleanData(value.toString());
		String line = value.toString();
		boolean result = parseLine(line,context);
		if(!result){
			return;
		}
		String sline = cleandata(line);
		context.write(new Text(sline),NullWritable.get());
	}

	private boolean parseLine(String line, Context context) {
		String[]data = line.split("\t");
		if(data.length==6){
			context.getCounter("map", "true").increment(1);
			return true;
		}else{
			System.out.println("------------------------------------------");
			System.out.println("error data: "+data[0]);
			System.out.println("------------------------------------------");
			context.getCounter("map", "false").increment(1);
			return false;
		}
	}
	
	private String cleandata(String str) {
		String[] data = str.split("\t");
		String line = "";
		//当数据长度不满足时，垃圾数据，打印第一列的id，并正常退出方法 
//		if(data.length != 6){
//			System.out.println("------------------------------------------");
//			System.out.println("error data: "+data[0]);
//			System.out.println("------------------------------------------");
//			System.exit(0);
//		}
		//去掉第四列的&左右的空格
		if(data[3].contains("&")){
			String[] type=data[3].split(" ");
			data[3]=type[0]+type[1]+type[2];
			//System.out.println(data[3]);
		}
		//把数据的tab符用|替换
		for (String ch : data) {
			line += ch+"|";
		}
		return line.substring(0, line.length()-1);
	}

}
