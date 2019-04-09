package com.project.etl.treatment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.project.etl.beans.Query;

public class Tree implements Runnable {

	private Query query;
	@Value("${dir.datasets}")
	private String pathdir;
	private String ext;
	private Dataset<Row> df;
	
	@Autowired
	private TreatmentService service;
	
	public Tree(Query q) {
		this.query = q;
	}
	
	@Override
	public void run() {
		this.df = this.execute(query);
	}
	
	public Dataset<Row> getDataFrame(){
		return this.df;
	}
	
	public Dataset<Row> execute(Query query){
		Dataset<Row> df1 = null;
		Dataset<Row> df2 = null;
		Tree tree1;
		Tree tree2;
		
		if(query.getPrevious1() != null && query.getPrevious2()!=null) { // combine case
			tree1 = new Tree(query.getPrevious1());
			tree2 = new Tree(query.getPrevious2());
			Thread t1 = new Thread(tree1);
			Thread t2 = new Thread(tree2);
			t1.start();
			t2.start();
			try {
				t1.join();
				t2.join();
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			df1=tree1.getDataFrame(); // run previous 1 (select)
			df1.show();
			df2=tree2.getDataFrame();// run previous 2 (selec)
			df2.show();
			return service.performSQLOpsOnDFs(df1, df2, query.getSelect(), query.getFilter());
			
		}else if(query.getPrevious1() != null) { // select or file
			
			if(query.getPreviousFile1() == null){ // combine case
				df1 =  execute(query.getPrevious1()); 
				df1.show();
				df1 = service.performSQLOpsOnDF(df1, query.getSelect(), query.getFilter());
				df1.show();
				return df1;
			}
			else { // (select or combine) + file 
				this.ext = service.getFileExtention(query.getPreviousFile1());
				if(ext.compareTo("xlsx")==0) df1 = service.getXslxDF(this.pathdir + query.getPreviousFile1());
				else if(ext.compareTo("csv")==0) df1 = service.getCsvDF(this.pathdir + query.getPreviousFile1());
				tree1 = new Tree(query.getPrevious1());
				df2 = execute(query.getPrevious1()); 
				return service.performSQLOpsOnDFs(df1, df2, query.getSelect(), query.getFilter());
			}
		}
		else {
			if(query.getPreviousFile1() != null && query.getPreviousFile2() != null) { // 2 file -> combine case
				
				df1 = service.getDFfile(this.pathdir + query.getPreviousFile1(), query.getPreviousFile1());
				df1.show();
				df2 = service.getDFfile(this.pathdir + query.getPreviousFile2(), query.getPreviousFile2());
				df2.show();
				return service.performSQLOpsOnDFs(df1, df2, query.getSelect(), query.getFilter());
				
			}else if(query.getPreviousFile1() != null) { // 1 file -> select
				
				System.out.println("File : "+query.getPreviousFile1());
				this.ext = service.getFileExtention(query.getPreviousFile1());
				if(ext.compareTo("xlsx")==0) {
					return service.performQueryOnXlsx(query.getSelect(), query.getFilter(), this.pathdir + query.getPreviousFile1());
				}
				else if(ext.compareTo("csv")==0) {
					return service.performQueryOnCSV(query.getSelect(), query.getFilter(), this.pathdir + query.getPreviousFile1());
				}
			}
		}		
		return null; // logically not reachable
	}
	
}
