package hadoop_mapreduce_descriptivestatistics.HadoopDescriptiveStatistics;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DescriptiveStatistics {

	public static class DSMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		int l = 0;
		private Text gt = new Text();
		private DoubleWritable accelerationWritable = new DoubleWritable(0);
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String data = value.toString();
		    String[] field = data.split(",");
		    double acceleration = 0;
		    if(field!=null && field.length == 10 && field[3].length()>0 && isNumeric(field[3]) && l>MainProgramWithGUI.first_instance && l<MainProgramWithGUI.last_instance) {
		    	acceleration = Double.parseDouble(field[3]);
		    	accelerationWritable.set(acceleration);
		        gt.set(field[9]);
		        context.write(gt, accelerationWritable);
		    }
		    l++;
		}

	}
  
    public static class DSReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        ArrayList<Double> accelerationList = new ArrayList<Double>();
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws java.io.IOException, InterruptedException {
        	accelerationList.clear();
        	for (DoubleWritable value : values) {
	        	accelerationList.add(value.get());
	        }
        	double result = 0;
        	if(MainProgramWithGUI.selected_function.equals("Mean")) {
        		result = mean(accelerationList);
        	}
        	else if(MainProgramWithGUI.selected_function.equals("Mode")) {
        		result = mode(accelerationList);
        	}
        	else if(MainProgramWithGUI.selected_function.equals("Median")) {
        		result = median(accelerationList);
        	}
        	else if(MainProgramWithGUI.selected_function.equals("Standard Deviation")) {
        		result = standard_deviation(accelerationList);
        	}
        	else {
        		result = range(accelerationList);
        	}
	        context.write(key, new DoubleWritable(result));
        }
    }
    
	public static boolean isNumeric(String str) {
	    NumberFormat formatter = NumberFormat.getInstance();
		ParsePosition pos = new ParsePosition(0);
		formatter.parse(str, pos);
		return str.length() == pos.getIndex();
	}
    
    public static double mean(ArrayList<Double> samples) {
		double sum = 0;
		for(int i=0;i<samples.size();i++) {
			sum = sum + samples.get(i);
		}
		double mean = sum / samples.size();
		return mean;
	}
	
	public static double mode(ArrayList<Double> samples) {
		double max_count = 0;
		double mode = samples.get(0);
		for(int i=0;i<samples.size();i++) {
			int count = 0;
			for(int j=0;j<samples.size();j++) {
				if(samples.get(i).equals(samples.get(j))) {
					count++;
				}
			}
			if(count > max_count) {
				max_count = count;
				mode = samples.get(i);
			}
		}
		return mode;
	}
	
	public static double median(ArrayList<Double> samples) {
		ArrayList<Double> arrayListForSort = new ArrayList<Double>();
		int n = samples.size();
		for(int i=0;i<n;i++) {
			arrayListForSort.add(samples.get(i));
		}
        for(int i=1;i<n;++i) { 
            double key = arrayListForSort.get(i); 
            int j = i - 1; 
            while(j >= 0 && arrayListForSort.get(j) > key) { 
            	arrayListForSort.set(j + 1, arrayListForSort.get(j));
                j = j - 1; 
            } 
            arrayListForSort.set(j + 1, key); 
        }
        if(n % 2 == 1) {
        	return arrayListForSort.get((n-1)/2);
        }
        else {
        	double median = (arrayListForSort.get(n/2) + arrayListForSort.get((n/2)-1)) / 2;
        	return median;
        }
	}
	
	public static double standard_deviation(ArrayList<Double> samples) {
		double standardDeviation = 0.0;
        int length = samples.size();
        double mean = mean(samples);
        for(double num: samples) {
            standardDeviation = standardDeviation + Math.pow(num - mean, 2);
        }
        return Math.sqrt(standardDeviation/length);
	}
	
	public static double range(ArrayList<Double> samples) {
		double max = samples.get(0);
		double min = samples.get(0);
		for(int i=0;i<samples.size();i++) {
			if(samples.get(i) > max) {
				max = samples.get(i);
			}
			if(samples.get(i) < min) {
				min = samples.get(i);
			}
		}
		double range = max - min;
		return range;
	}
	
}
