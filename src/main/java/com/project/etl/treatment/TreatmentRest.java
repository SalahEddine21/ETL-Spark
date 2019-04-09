package com.project.etl.treatment;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.List;

import javax.servlet.http.HttpSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.project.etl.beans.ApplicationContextProvider;
import com.project.etl.beans.Query;

@RestController
@CrossOrigin("*")
@RequestMapping("/file")
public class TreatmentRest{

	@Value("${dir.datasets}")
	private String pathdir;
	
	@Autowired
	private TreatmentService service;
	@Autowired
	private ApplicationContextProvider provider;
	
	@RequestMapping(value="/columns", method=RequestMethod.POST)
	public ResponseEntity<List<String>> getColumns(@RequestPart("file") MultipartFile file){
		String filename = file.getOriginalFilename();
		String fileExt = filename.substring(filename.lastIndexOf('.') + 1);
		List<String> columns = null;
		System.out.println(file.getOriginalFilename() + "--" + fileExt);
		try {
	        byte barr[]=file.getBytes();  
	        //BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(pathdir+filename));  
	        FileOutputStream fos = (FileOutputStream) provider.getApplicationContext().getBean("fileOutputStream", pathdir+filename);
	        BufferedOutputStream bout = (BufferedOutputStream) provider.getApplicationContext().getBean("bufferedOutputStream", fos);
	        bout.write(barr);  
	        bout.flush();  
	        bout.close(); 
	        if(fileExt.compareTo("xlsx")==0) columns = service.getXlsxColumns(pathdir+filename);
	        else if(fileExt.compareTo("csv") == 0) columns = service.getCsvColumns(pathdir+filename);
	        return new ResponseEntity<List<String>>(columns, HttpStatus.OK);
		} catch (Exception e) {
			System.out.println("EXCEPTION IS: "+e.getMessage());
			return new ResponseEntity<List<String>>(columns, HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@RequestMapping(value="/run", method=RequestMethod.POST)
	public ResponseEntity<List<List<Object>>> runIt(@RequestBody Query query){
		List<List<Object>> rows = null;
		try {
			Dataset<Row> df = service.run(query); // run algorithme
			rows = service.convertDataToStrings(df); // get rows 
		} catch (Exception e) {
			System.out.println("EXCEPTION IS: "+e.getMessage());
		}
		return new ResponseEntity<List<List<Object>>>(rows, HttpStatus.OK);
	}
	
}
