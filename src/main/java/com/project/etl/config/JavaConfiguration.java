package com.project.etl.config;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.project.etl.beans.ApplicationContextProvider;

@Configuration
public class JavaConfiguration {

	@Bean(name="provider")
	public ApplicationContextProvider getApplicationContext_Bean() {
		return new ApplicationContextProvider();
	}
	
	@Bean(name="bufferedOutputStream")
	@Scope("prototype")
	public BufferedOutputStream bufferedOutputStream(FileOutputStream file) {
		return new BufferedOutputStream(file);
	}
	
	@Bean(name="fileOutputStream")
	@Scope("prototype")
	public FileOutputStream FileOutputStream(String filepath) {
		try {
			return new FileOutputStream(filepath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	@Bean(name="javaSparkContext")
	@Scope("prototype")
	public JavaSparkContext javaSparkContext(SparkConf conf ) {
		return new JavaSparkContext(conf);
	}
	
	
}
