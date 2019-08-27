package xyz.ylq.bigdata.datacleanmapreduce;

public class DataClean {

	public static String cleanData(String str) {
		String[] data = str.split("\t");
		String line = "";
		//当数据长度不满足时，垃圾数据，打印第一列的id，并正常退出方法 
		if(data.length != 6){
			System.out.println("------------------------------------------");
			System.out.println("error data: "+data[0]);
			System.out.println("------------------------------------------");
			System.exit(0);
		}
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
	
//	public static void main(String[] args) {
//		System.out.println(cleanData("MEvoy_owET8	smpfilms	736	Travel & Places	tall	3.1415926"));
//	}

}
