package org.OpenGeoPortal.Ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.OpenGeoPortal.Utilities.ZipFilePackager;

public class BasicUploadMetadataJob extends AbstractMetadataJob implements UploadMetadataJob, Runnable 
{
	
	/**
	 * delete uploaded files
	 */
	@Override
	public void cleanUp()
	{
		try 
		{
			File parentDir = fgdcFile.get(0).getParentFile();
			for (File file : fgdcFile)
			{
				file.delete();
			}
			parentDir.delete();
		}
		catch (Exception e)
		{
			ingestStatus.addError("cleanup", "Failed to delete temp files");
		}	
	}
	
	/**
	 * loop over uploaded data and process individual files
	 */
	@Override
	public void getMetadata() throws IOException{
		ingestStatus = ingestStatusManager.getIngestStatus(jobId);
		int totalFileCount = fgdcFile.size();
		int fileCounter = 0;
		for (File file: fgdcFile){
			//decide what to do with the file(s)
			fileCounter++;
			ingestStatus.setProgress(fileCounter, totalFileCount);
			processFile(file, totalFileCount, null);
		}
		if (ingestStatus.getErrors().isEmpty()){
			ingestStatus.setJobStatus(IngestJobStatus.Succeeded);
		} else {
			ingestStatus.setJobStatus(IngestJobStatus.Finished);
		}
	}
	
}
