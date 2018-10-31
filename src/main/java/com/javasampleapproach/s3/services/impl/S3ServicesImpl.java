package com.javasampleapproach.s3.services.impl;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.javasampleapproach.s3.services.S3Services;
import com.javasampleapproach.s3.util.Utility;

@Service
public class S3ServicesImpl implements S3Services {
	
	private Logger logger = LoggerFactory.getLogger(S3ServicesImpl.class);
	
	@Autowired
	private AmazonS3 s3client;

	@Value("${jsa.s3.bucket}")
	private String bucketName;
	
	@Value("${jsa.s3.date}")
	private String date;

	@Override
	public void downloadFile(String keyName) {

	}

	@Override
	public void uploadFile(String uploadFolderPath) {
		File folder = new File(uploadFolderPath);
		/*File[] files = folder.listFiles();
		int i = 1;
		for (File file : files) {
			logger.info("Upload File - " + (i++) + " of " + files.length + " -> " + file.getName());
			uploadFile(file);
		}*/
		
		String[] path = new String[3];
		
		for (File year : folder.listFiles()) {
			path[0] = year.getName();
			if (!year.isDirectory()){
				continue;
			}
			logger.info("Started year: " + year.getName());
			
			for (File month : year.listFiles()){
				path[1] = month.getName();
				if (!month.isDirectory()){
					continue;
				}
				logger.info("Started month: " + year.getName() + month.getName());
				
				for (File day : month.listFiles()){
					path[2] = day.getName();
					if (!day.isDirectory()){
						continue;
					}
					logger.info("Started day: " + year.getName() + month.getName() + day.getName());
					File[] files = day.listFiles();
					int i = 1;
					for (File file : files){
						uploadFile(file, path);
						logger.info("Upload File - " + (i++) + " of " + files.length + " -> " + file.getName());
					}
					logger.info("Finished day: " + year.getName() + month.getName() + day.getName());
				}
				logger.info("Finished month: " + year.getName() + month.getName());
			}
			logger.info("Finished year: " + year.getName());
		}		
	}

	private void uploadFile(File file, String[] path) {
		try {

	        String fileName = file.getName().substring(file.getName().indexOf("_") + 1);
	        
			s3client.putObject(new PutObjectRequest(bucketName, date + "/" + fileName, file));
	        
	        
		} catch (AmazonServiceException ase) {
			logger.info("Caught an AmazonServiceException from PUT requests, rejected reasons:");
			logger.info("Error Message:    " + ase.getMessage());
			logger.info("HTTP Status Code: " + ase.getStatusCode());
			logger.info("AWS Error Code:   " + ase.getErrorCode());
			logger.info("Error Type:       " + ase.getErrorType());
			logger.info("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            logger.info("Caught an AmazonClientException: ");
            logger.info("Error Message: " + ace.getMessage());
        }
	}

}
