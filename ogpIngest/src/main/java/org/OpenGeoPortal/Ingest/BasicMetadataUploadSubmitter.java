package org.OpenGeoPortal.Ingest;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.OpenGeoPortal.Ingest.AbstractSolrIngest.MetadataElement;

public class BasicMetadataUploadSubmitter extends AbstractIngestJobSubmitter implements MetadataUploadSubmitter {

	/**
	 * if a url is provided, we start crawl metadata job, otherwise start an upload metadata job
	 */
	public UUID runIngestJob
		(String sessionId, String institution, Set<MetadataElement> requiredFields, String options, 
				List<File> fgdcFiles, String url) {
		UUID jobId = registerJob(sessionId);
		UploadMetadataJob ingestJob;
		if (url == null) 
			url = "";
		url = url.trim();
		if (url.length() == 0)
		{
			ingestJob = (UploadMetadataJob) beanFactory.getBean("uploadMetadataJob");
			ingestJob.init(jobId, institution, requiredFields, options, fgdcFiles, url);			
		}
		else
		{
			ingestJob = (UploadMetadataJob) beanFactory.getBean("crawlMetadataJob");
			ingestJob.init(jobId, institution, requiredFields, options, fgdcFiles, url);			
		}

		asyncTaskExecutor.execute(ingestJob);
	return jobId;
	}
}
