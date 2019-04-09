package com.project.etl.treatment;

import java.io.FileInputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.bag.SynchronizedBag;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVParser;
//import org.apache.commons.csv.CSVRecord;
//import org.apache.poi.ss.usermodel.Cell;
//import org.apache.poi.ss.usermodel.Row;
//import org.apache.poi.xssf.usermodel.XSSFSheet;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.project.etl.beans.ApplicationContextProvider;
import com.project.etl.beans.Query;

import scala.collection.JavaConversions;

@Service
public class TreatmentService {

	private SparkConf conf = null;
	private JavaSparkContext sc = null;
	
	@Autowired
	private ApplicationContextProvider provider;
	
	@Value("${dir.datasets}")
	private String pathdir;
	private String ext;
	
	// get columns of xlsx files
	public List<String> getXlsxColumns(String pathfile){
		try {
			SparkSession spark = SparkSession
				    .builder()
				    .appName("Java Spark SQL Example")
				    .config("spark.master", "local")
				    .getOrCreate();
			
			Dataset<Row> df = 
		            spark.read().
		            format("com.crealytics.spark.excel").
		            option("useHeader", true).
		            option("treatEmptyValuesAsNulls", false).
		            option("inferSchema", true).
		            load(pathfile);
			List<String> columns = new ArrayList<String>();
			for (String column : df.columns()) {
				columns.add(column);
				System.out.println(column);
			}
			spark.close();
			return columns;
		} catch (Exception e) {
			System.out.println("EXCEPTION IS: "+e.getMessage());
			return null;
		}
	}
	
	// get columns of csv files
	public List<String> getCsvColumns(String pathfile){ 
		try {
			SparkConf conf = this.getSparkConfig();
	        JavaSparkContext sc = (JavaSparkContext) provider.getApplicationContext().getBean("javaSparkContext", conf);
	        JavaRDD<String> allRows = sc.textFile(pathfile);
	        System.out.println(allRows.take(5));
	        List<String> headers = Arrays.asList(allRows.take(1).get(0).split(";"));
			for (String string : headers) {
				System.out.println(string);
			}
			sc.close();
			return headers;
		} catch (Exception e) {
			System.out.println("EXCEPTION IS: "+e.getMessage());
			return null;
		}
	}
	
	private SparkConf getSparkConfig() {
		if(this.conf == null) return new SparkConf().setAppName("CSV Reader").setMaster("local[*]");
		return this.conf;
	}
	
	// perform sql query on csv file
	public Dataset<Row> performQueryOnCSV(String select, String filter, String pathfile) { 
		SparkSession spark = SparkSession.builder()
				.appName("Spark CSV Analysis Demo")
				.master("local[5]")
			    .getOrCreate();

		Dataset<Row> df = 
	            spark.read().
	            option("delimiter",";").
	            option("header", true).
	            csv(pathfile);
		
		df.createOrReplaceTempView("TMP_DATA");
		return spark.sql(select + " FROM TMP_DATA "+ filter);
		//df.printSchema();
	}
	
	// perform sql query on xlsx file
	public Dataset<Row> performQueryOnXlsx(String select, String filter, String pathfile) { 
		SparkSession spark = SparkSession
			    .builder()
			    .appName("Java Spark SQL Example")
			    .config("spark.master", "local")
			    .getOrCreate();
		
		Dataset<Row> df = 
	            spark.read().
	            format("com.crealytics.spark.excel").
	            option("useHeader", true).
	            option("treatEmptyValuesAsNulls", false).
	            option("inferSchema", true).
	            load(pathfile);
		
		df.createOrReplaceTempView("TMP_DATA");
		return spark.sql(select + " FROM TMP_DATA "+ filter);
	}
	
	// get dataframes of xlsx file
	synchronized public Dataset<Row> getXslxDF(String pathfile){ 
		SparkSession spark = SparkSession
			    .builder()
			    .appName("Java Spark SQL Example")
			    .config("spark.master", "local")
			    .getOrCreate();
		
		Dataset<Row> df = 
	            spark.read().
	            format("com.crealytics.spark.excel").
	            option("useHeader", true).
	            option("treatEmptyValuesAsNulls", false).
	            option("inferSchema", true).
	            load(pathfile);
		return df;
	}
	
	// get dataframes of csv file
	synchronized public Dataset<Row> getCsvDF(String pathfile){ 
		SparkSession spark = SparkSession.builder()
				.appName("Spark CSV Analysis Demo")
				.master("local[5]")
			    .getOrCreate();

		Dataset<Row> df = 
	            spark.read().
	            option("delimiter",";").
	            option("header", true).
	            csv(pathfile);
		return df;
	}
	
	// perform sql queries of two given dataframes
	public Dataset<Row> performSQLOpsOnDFs(Dataset<Row> df1, Dataset<Row> df2, String select, String filter){
		SparkSession spark = SparkSession
			    .builder()
			    .appName("Java Spark SQL Example")
			    .config("spark.master", "local")
			    .getOrCreate();
		
		df1.createOrReplaceTempView("src");
		df2.createOrReplaceTempView("srcc");
		
		return spark.sql("SELECT "+select + " FROM src s JOIN srcc c ON "+filter );
		
	}
	
	// perform sql queries on given dataframe
	public Dataset<Row> performSQLOpsOnDF(Dataset<Row> df, String select, String filter){
		SparkSession spark = SparkSession.builder()
				.appName("Spark CSV Analysis Demo")
				.master("local[5]")
			    .getOrCreate();
		df.createOrReplaceTempView("view");
		return spark.sql(select + " FROM view " + filter);
	}
	
	// Algorithm 
	synchronized public Dataset<Row> run(Query query) {  
		Dataset<Row> df1 = null;
		Dataset<Row> df2 = null;
		
		if(query.getPrevious1() != null && query.getPrevious2()!=null) { // combine case
			
			df1=run(query.getPrevious1()); // run previous 1 (select)
			df1.show();
			df2=run(query.getPrevious2()); // run previous 2 (selec)
			df2.show();
			return this.performSQLOpsOnDFs(df1, df2, query.getSelect(), query.getFilter());
			
		}else if(query.getPrevious1() != null) { // select or file
			
			if(query.getPreviousFile1() == null){ // combine case
				df1 =  run(query.getPrevious1()); 
				df1.show();
				df1 = this.performSQLOpsOnDF(df1, query.getSelect(), query.getFilter());
				df1.show();
				return df1;
			}
			else { // (select or combine) + file 
				this.ext = this.getFileExtention(query.getPreviousFile1());
				if(ext.compareTo("xlsx")==0) df1 = this.getXslxDF(this.pathdir + query.getPreviousFile1());
				else if(ext.compareTo("csv")==0) df1 = this.getCsvDF(this.pathdir + query.getPreviousFile1());
				df2 = run(query.getPrevious1()); 
				return this.performSQLOpsOnDFs(df1, df2, query.getSelect(), query.getFilter());
			}
		}
		else {
			if(query.getPreviousFile1() != null && query.getPreviousFile2() != null) { // 2 file -> combine case
				
				df1 = this.getDFfile(this.pathdir + query.getPreviousFile1(), query.getPreviousFile1());
				df1.show();
				df2 = this.getDFfile(this.pathdir + query.getPreviousFile2(), query.getPreviousFile2());
				df2.show();
				return this.performSQLOpsOnDFs(df1, df2, query.getSelect(), query.getFilter());
				
			}else if(query.getPreviousFile1() != null) { // 1 file -> select
				
				System.out.println("File : "+query.getPreviousFile1());
				this.ext = this.getFileExtention(query.getPreviousFile1());
				if(ext.compareTo("xlsx")==0) {
					return this.performQueryOnXlsx(query.getSelect(), query.getFilter(), this.pathdir + query.getPreviousFile1());
				}
				else if(ext.compareTo("csv")==0) {
					return this.performQueryOnCSV(query.getSelect(), query.getFilter(), this.pathdir + query.getPreviousFile1());
				}
			}
		}		
		return null; // logically not reachable
	}
	
	public String getFileExtention(String filename) {
		return filename.substring(filename.lastIndexOf('.') + 1);
	}
	
	// get file dataframe
	public Dataset<Row> getDFfile(String filepath, String filename){
		String ext = this.getFileExtention(filename);
		if(ext.compareTo("xlsx")==0) return this.getXslxDF(filepath);
		else return this.getCsvDF(filepath);
	}
	
	// convert dataframes to rows
	public List<List<Object>> convertDataToStrings(Dataset<Row> df){
		List<List<Object>> data = new ArrayList<List<Object>>();
		List<Object> columns = new ArrayList<Object>();
		
		for (String column : df.columns()) {
			columns.add(column);
		}
		data.add(columns); // data columns
		List<Object> values = null;
		List<Row> rows = df.collectAsList();
		for (Row row : rows) {
			values = JavaConversions.seqAsJavaList(row.toSeq());
			data.add(values);
		}
		return data;
	}
}
