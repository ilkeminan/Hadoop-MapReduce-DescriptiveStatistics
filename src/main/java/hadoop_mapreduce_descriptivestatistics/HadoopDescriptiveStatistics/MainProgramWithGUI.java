package hadoop_mapreduce_descriptivestatistics.HadoopDescriptiveStatistics;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import javax.swing.JButton;
import javax.swing.JComboBox;
import java.awt.event.*;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JTextField;

public class MainProgramWithGUI extends Configured implements Tool{
	static long first_instance, last_instance;
    static String input_path, output_path, selected_function;
	
	public static void main(final String[] args) {
		final Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		final JFrame frame = new JFrame("Mapreduce Descriptive Statistics");
		
	    JButton button_mkdir = new JButton("Make Directory (mkdir)");
	    button_mkdir.setBounds(0,60,500,30);
	    button_mkdir.addActionListener(new ActionListener(){  
	        public void actionPerformed(ActionEvent e){  
	        	final JFrame frame_mkdir = new JFrame("Make Directory (mkdir)");
	        	frame.setVisible(false);
	        	frame_mkdir.setVisible(true);
	        	frame_mkdir.setBounds(500, 100, 500, 400);
	    	    frame_mkdir.setLayout(null);
	    	    JLabel label = new JLabel();
	    	    label.setBounds(50,100,100,30);
	    	    label.setText("Directory:");
	    	    frame_mkdir.add(label);
	    	    final JTextField textField = new JTextField(20);
	    	    textField.setBounds(150,100,300,30);
	    	    frame_mkdir.add(textField);
	    	    JButton button_start = new JButton("Make Directory");  
	    	    button_start.setBounds(50,130,200,30);
	    	    button_start.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		String uri = "hdfs://localhost:9000/" + textField.getText().toString();
	    	    		try {
	    	    			FileSystem fs = FileSystem.get(URI.create(uri), conf);
		    	    		fs.mkdirs(new Path(uri));
	    	    		}
	    	    		catch(Exception ex) {
	    	    			
	    	    		}
	    	    		frame_mkdir.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_mkdir.add(button_start);
	    	    JButton buttonToMainWindow = new JButton("Go to Main Window");  
	    	    buttonToMainWindow.setBounds(250,130,200,30);
	    	    buttonToMainWindow.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		frame_mkdir.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_mkdir.add(buttonToMainWindow);
	        }
	    });
	    frame.add(button_mkdir);
	    
	    JButton button_ls = new JButton("List Files and Directories (ls)");
	    button_ls.setBounds(0,90,500,30); 
	    button_ls.addActionListener(new ActionListener(){  
	        public void actionPerformed(ActionEvent e){  
	        	final JFrame frame_ls = new JFrame("List Files and Directories (ls)");
	        	frame.setVisible(false);
	        	frame_ls.setVisible(true);
	        	frame_ls.setBounds(500, 100, 500, 400);
	    	    frame_ls.setLayout(null);
	    	    JLabel label = new JLabel();
	    	    label.setBounds(50,100,100,30);
	    	    label.setText("Directory:");
	    	    frame_ls.add(label);
	    	    final JTextField textField = new JTextField(20);
	    	    textField.setBounds(150,100,300,30);
	    	    frame_ls.add(textField);
	    	    final String[] listToShow = new String[100];
	    	    final JList list = new JList(listToShow);
	    	    list.setBounds(50,160,450,500);
	    	    frame_ls.add(list);
	    	    JButton button_start = new JButton("List Files and Directories");  
	    	    button_start.setBounds(50,130,200,30);
	    	    button_start.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		String uri = "hdfs://localhost:9000/" + textField.getText().toString();
	    	    		FileStatus [] files = null;
	    	    		try {
	    	    			FileSystem fs = FileSystem.get(URI.create(uri), conf);
		    	    		files = fs.listStatus(new Path(uri));
	    	    		}
	    	    		catch(Exception ex) {
	    	    			
	    	    		}
	    	    		for(int i=0;i<listToShow.length;i++) {
	    	    			listToShow[i] = "";                        //remove the previous items
	    	    		}
	    	    		int i = 0;
	    	    		for(FileStatus file:files) {
	    	    			listToShow[i] = file.getPath().getName();
	    	    			i++;
	    	    		}
	    	    		list.setListData(listToShow);
	    	    	}
	    	    });
	    	    frame_ls.add(button_start);
	    	    JButton buttonToMainWindow = new JButton("Go to Main Window");  
	    	    buttonToMainWindow.setBounds(250,130,200,30);
	    	    buttonToMainWindow.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		frame_ls.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_ls.add(buttonToMainWindow);
	        }  
	    });
	    frame.add(button_ls);
	    
	    JButton button_copyFromLocal = new JButton("Copy from Local File (copyFromLocal)");
	    button_copyFromLocal.setBounds(0,120,500,30);
	    button_copyFromLocal.addActionListener(new ActionListener(){  
	        public void actionPerformed(ActionEvent e){  
	        	final JFrame frame_copyFromLocal = new JFrame("Copy from Local File (copyFromLocal)");
	        	frame.setVisible(false);
	        	frame_copyFromLocal.setVisible(true);
	        	frame_copyFromLocal.setBounds(500, 100, 500, 400);
	    	    frame_copyFromLocal.setLayout(null);
	    	    JLabel label_local = new JLabel();
	    	    label_local.setBounds(50,100,100,30);
	    	    label_local.setText("Local path:");
	    	    frame_copyFromLocal.add(label_local);
	    	    final JTextField textField_local = new JTextField(20);
	    	    textField_local.setBounds(150,100,300,30);
	    	    frame_copyFromLocal.add(textField_local);
	    	    JLabel label_hdfs = new JLabel();
	    	    label_hdfs.setBounds(50,130,100,30);
	    	    label_hdfs.setText("HDFS Directory:");
	    	    frame_copyFromLocal.add(label_hdfs);
	    	    final JTextField textField_hdfs = new JTextField(20);
	    	    textField_hdfs.setBounds(150,130,300,30);
	    	    frame_copyFromLocal.add(textField_hdfs);
	    	    JButton button_start = new JButton("Copy from Local File");  
	    	    button_start.setBounds(50,160,200,30);
	    	    button_start.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		String localPath =  textField_local.getText().toString();
	    	    		String uri = "hdfs://localhost:9000";
	    	    		String hdfsDir = "hdfs://localhost:9000/" + textField_hdfs.getText().toString();
	    	    		try {
	    	    			FileSystem fs = FileSystem.get(URI.create(uri), conf);
		    	    		fs.copyFromLocalFile(new Path(localPath), new Path(hdfsDir));
	    	    		}
	    	    		catch(Exception ex) {
	    	    			
	    	    		}
	    	    		frame_copyFromLocal.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_copyFromLocal.add(button_start);
	    	    JButton buttonToMainWindow = new JButton("Go to Main Window");  
	    	    buttonToMainWindow.setBounds(250,160,200,30);
	    	    buttonToMainWindow.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		frame_copyFromLocal.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_copyFromLocal.add(buttonToMainWindow);
	        }  
	    });
	    frame.add(button_copyFromLocal);
	    
	    JButton button_copyToLocal = new JButton("Copy to Local File (copyToLocal)");
	    button_copyToLocal.setBounds(0,150,500,30);
	    button_copyToLocal.addActionListener(new ActionListener(){  
	        public void actionPerformed(ActionEvent e){  
	        	final JFrame frame_copyToLocal = new JFrame("Copy to Local File (copyToLocal)");
	        	frame.setVisible(false);
	        	frame_copyToLocal.setVisible(true);
	        	frame_copyToLocal.setBounds(500, 100, 500, 400);
	    	    frame_copyToLocal.setLayout(null);
	    	    JLabel label_hdfs = new JLabel();
	    	    label_hdfs.setBounds(50,100,100,30);
	    	    label_hdfs.setText("HDFS Directory:");
	    	    frame_copyToLocal.add(label_hdfs);
	    	    final JTextField textField_hdfs = new JTextField(20);
	    	    textField_hdfs.setBounds(150,100,300,30);
	    	    frame_copyToLocal.add(textField_hdfs);
	    	    JLabel label_local = new JLabel();
	    	    label_local.setBounds(50,130,100,30);
	    	    label_local.setText("Local path:");
	    	    frame_copyToLocal.add(label_local);
	    	    final JTextField textField_local = new JTextField(20);
	    	    textField_local.setBounds(150,130,300,30);
	    	    frame_copyToLocal.add(textField_local);
	    	    JButton button_start = new JButton("Copy to Local File");  
	    	    button_start.setBounds(50,160,200,30);
	    	    button_start.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		String localPath = textField_local.getText().toString();
	    	    		String uri = "hdfs://localhost:9000";
	    	    		String hdfsDir = "hdfs://localhost:9000/" + textField_hdfs.getText().toString();
	    	    		try {
	    	    			FileSystem fs = FileSystem.get(URI.create(uri), conf);
		    	    		fs.copyToLocalFile(new Path(hdfsDir), new Path(localPath));
	    	    		}
	    	    		catch(Exception ex) {
	    	    			
	    	    		}
	    	    		frame_copyToLocal.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_copyToLocal.add(button_start);
	    	    JButton buttonToMainWindow = new JButton("Go to Main Window");  
	    	    buttonToMainWindow.setBounds(250,160,200,30);
	    	    buttonToMainWindow.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		frame_copyToLocal.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_copyToLocal.add(buttonToMainWindow);
	        }  
	    });
	    frame.add(button_copyToLocal);
	    
	    JButton button_rm = new JButton("Remove File or Directories (rm)");
	    button_rm.setBounds(0,180,500,30);
	    button_rm.addActionListener(new ActionListener(){  
	        public void actionPerformed(ActionEvent e){  
	        	final JFrame frame_rm = new JFrame("Remove File or Directories (rm)");
	        	frame.setVisible(false);
	        	frame_rm.setVisible(true);
	        	frame_rm.setBounds(500, 100, 500, 400);
	    	    frame_rm.setLayout(null);
	    	    JLabel label = new JLabel();
	    	    label.setBounds(50,100,100,30);
	    	    label.setText("Directory:");
	    	    frame_rm.add(label);
	    	    final JTextField textField = new JTextField(20);
	    	    textField.setBounds(150,100,300,30);
	    	    frame_rm.add(textField);
	    	    JButton button_start = new JButton("Remove File or Directories");  
	    	    button_start.setBounds(50,130,200,30);
	    	    button_start.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		String uri = "hdfs://localhost:9000/" + textField.getText().toString();
	    	    		try {
	    	    			FileSystem fs = FileSystem.get(URI.create(uri), conf);
		    	    		fs.delete(new Path(uri), true);
	    	    		}
	    	    		catch(Exception ex) {
	    	    			
	    	    		}
	    	    		frame_rm.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_rm.add(button_start);
	    	    JButton buttonToMainWindow = new JButton("Go to Main Window");  
	    	    buttonToMainWindow.setBounds(250,130,200,30);
	    	    buttonToMainWindow.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		frame_rm.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_rm.add(buttonToMainWindow);
	        }  
	    });
	    frame.add(button_rm);
	    
	    JButton button_cat = new JButton("Read Files (cat)");
	    button_cat.setBounds(0,210,500,30);
	    button_cat.addActionListener(new ActionListener(){
	        public void actionPerformed(ActionEvent e){  
	        	final JFrame frame_cat = new JFrame("Read Files (cat)");
	        	frame.setVisible(false);
	        	frame_cat.setVisible(true);
	        	frame_cat.setBounds(500, 100, 500, 400);
	    	    frame_cat.setLayout(null);
	    	    JLabel label = new JLabel();
	    	    label.setBounds(50,100,100,30);
	    	    label.setText("File:");
	    	    frame_cat.add(label);
	    	    final JTextField textField = new JTextField(20);
	    	    textField.setBounds(150,100,300,30);
	    	    frame_cat.add(textField);
	    	    final JList list = new JList();
	    	    list.setBounds(50,160,450,500);
	    	    frame_cat.add(list);
	    	    JButton button_start = new JButton("Read Files");  
	    	    button_start.setBounds(50,130,200,30);
	    	    button_start.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		String uri = "hdfs://localhost:9000/" + textField.getText().toString();
	    	    		InputStream in = null;
	    	    		try {
	    	    			FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    	    			in = fs.open(new Path(uri));
	    	    			ByteArrayOutputStream out = new ByteArrayOutputStream();
	    	    			IOUtils.copyBytes(in, out, 4096, false);
	    	    			String resultsString = new String(out.toByteArray());
	    	    			String[] results = resultsString.split("\n");
	    	    			list.setListData(results);
	    	    		}
	    	    		catch(Exception ex) {
	    	    			ex.printStackTrace();
	    	    		}
	    	    		finally {
	    	    			IOUtils.closeStream(in);
	    	    		}
	    	    	}
	    	    });
	    	    frame_cat.add(button_start);
	    	    JButton buttonToMainWindow = new JButton("Go to Main Window");  
	    	    buttonToMainWindow.setBounds(250,130,200,30);
	    	    buttonToMainWindow.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		frame_cat.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_cat.add(buttonToMainWindow);
	        }
	    });
	    frame.add(button_cat);
	    
	    JButton button_mapReduce = new JButton("MapReduce");
	    button_mapReduce.setBounds(0,240,500,30);
	    button_mapReduce.addActionListener(new ActionListener(){  
	        public void actionPerformed(ActionEvent e){  
	        	final JFrame frame_mapReduce = new JFrame("MapReduce");
	        	frame.setVisible(false);
	        	frame_mapReduce.setVisible(true);
	        	frame_mapReduce.setBounds(500, 100, 500, 400);
	    	    frame_mapReduce.setLayout(null);
	    	    JLabel label_first_instance = new JLabel();
	    	    label_first_instance.setBounds(50,70,150,30);
	    	    label_first_instance.setText("First instance number:");
	    	    frame_mapReduce.add(label_first_instance);
	    	    final JTextField textField_first_instance = new JTextField(20);
	    	    textField_first_instance.setBounds(200,70,250,30);
	    	    textField_first_instance.setText("0");
	    	    frame_mapReduce.add(textField_first_instance);
	    	    JLabel label_last_instance = new JLabel();
	    	    label_last_instance.setBounds(50,100,150,30);
	    	    label_last_instance.setText("Last instance number:");
	    	    frame_mapReduce.add(label_last_instance);
	    	    final JTextField textField_last_instance = new JTextField(20);
	    	    textField_last_instance.setBounds(200,100,250,30);
	    	    textField_last_instance.setText("300000");
	    	    frame_mapReduce.add(textField_last_instance);
	    	    JLabel label_input = new JLabel();
	    	    label_input.setBounds(50,130,100,30);
	    	    label_input.setText("Input path:");
	    	    frame_mapReduce.add(label_input);
	    	    final JTextField textField_input = new JTextField(20);
	    	    textField_input.setBounds(150,130,300,30);
	    	    frame_mapReduce.add(textField_input);
	    	    JLabel label_output = new JLabel();
	    	    label_output.setBounds(50,160,100,30);
	    	    label_output.setText("Output path:");
	    	    frame_mapReduce.add(label_output);
	    	    final JTextField textField_output = new JTextField(20);
	    	    textField_output.setBounds(150,160,300,30);
	    	    frame_mapReduce.add(textField_output);
	    	    JLabel label_statistics_functions = new JLabel();
	    	    label_statistics_functions.setBounds(50,190,200,30);
	    	    label_statistics_functions.setText("Descriptive Statistics Function:");
	    	    frame_mapReduce.add(label_statistics_functions);
	    	    String [] descriptive_statistics_functions = {"Mean", "Mode", "Median", "Standard Deviation", "Range"};
	    	    final JComboBox comboBox_statistics_functions = new JComboBox(descriptive_statistics_functions);
	    	    comboBox_statistics_functions.setSelectedIndex(0);
	    	    comboBox_statistics_functions.setBounds(250,190,200,30);
	    	    frame_mapReduce.add(comboBox_statistics_functions);
	    	    JButton button_start = new JButton("Start MapReduce");
	    	    button_start.setBounds(50,220,200,30);
	    	    button_start.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
						try {
							first_instance = Long.parseLong(textField_first_instance.getText().toString());
		    	    		last_instance = Long.parseLong(textField_last_instance.getText().toString());
							input_path = "hdfs://localhost:9000/" + textField_input.getText().toString();
							output_path = "hdfs://localhost:9000/" + textField_output.getText().toString();
							selected_function = comboBox_statistics_functions.getSelectedItem().toString();
							ToolRunner.run(new MainProgramWithGUI(), args);
						}
						catch (Exception ex) {
                            ex.printStackTrace();
						}
	    	    		frame_mapReduce.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_mapReduce.add(button_start);
	    	    JButton buttonToMainWindow = new JButton("Go to Main Window");  
	    	    buttonToMainWindow.setBounds(250,220,200,30);
	    	    buttonToMainWindow.addActionListener(new ActionListener() {
	    	    	public void actionPerformed(ActionEvent e2) {
	    	    		frame_mapReduce.setVisible(false);
	    	    		frame.setVisible(true);
	    	    	}
	    	    });
	    	    frame_mapReduce.add(buttonToMainWindow);
	        }  
	    });
	    frame.add(button_mapReduce);
	    
	    frame.setBounds(500, 100, 500, 400);
	    frame.setLayout(null);
	    frame.setVisible(true);		
	}

	public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "DescriptiveStatistics");
        job.setMapperClass(DescriptiveStatistics.DSMapper.class);
        job.setReducerClass(DescriptiveStatistics.DSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        return job.waitForCompletion(true) ? 0 : 1;
	}
}
