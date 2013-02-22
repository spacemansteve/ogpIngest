package org.OpenGeoPortal.Ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.OpenGeoPortal.Ingest.AbstractSolrIngest.MetadataElement;
import org.OpenGeoPortal.Ingest.IngestResponse.IngestInfo;
import org.OpenGeoPortal.Ingest.Metadata.MetadataConverter;
import org.OpenGeoPortal.Ingest.Metadata.MetadataParseResponse;
import org.OpenGeoPortal.Layer.Metadata;
import org.OpenGeoPortal.Utilities.ZipFilePackager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * classes that gather data for ingest inherit from this class
 * ingest doesn't run in the thread created by the http request
 * instead, a new thread is spawned.  progress is reported back to 
 * the browser via the IngestStatusManager.
 *
 */
public abstract class AbstractMetadataJob {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected IngestStatusManager ingestStatusManager;
	private MetadataConverter metadataConverter;
	private MapServerIngest mapServerIngest;
	private SolrIngest solrIngest;
	private ExtraTasks extraTasks;
	protected IngestProperties ingestProperties;
	protected String institution;
	protected String options;
	protected String url;  // url to crawl
	protected List<File> fgdcFile;
	protected UUID jobId;
	protected IngestStatus ingestStatus;
	private Set<MetadataElement> requiredFields;

	public AbstractMetadataJob() {
		super();
	}

	/* (non-Javadoc)
	 * @see org.OpenGeoPortal.Ingest.MetadataJob#init(java.util.UUID, java.lang.String, java.util.Set, java.lang.String, java.util.List)
	 */
	public void init(UUID jobId, String institution, Set<MetadataElement> requiredFields, String options,
			List<File> fgdcFile, String url) {
					this.institution = institution;
					this.options = options;
					this.fgdcFile = fgdcFile;
					this.jobId = jobId;
					this.requiredFields = requiredFields;
					this.url = url;
			}

	/* (non-Javadoc)
	 * @see org.OpenGeoPortal.Ingest.MetadataJob#setIngestStatusManager(org.OpenGeoPortal.Ingest.IngestStatusManager)
	 */
	public void setIngestStatusManager(IngestStatusManager ingestStatusManager) {
		this.ingestStatusManager = ingestStatusManager;
	}

	/* (non-Javadoc)
	 * @see org.OpenGeoPortal.Ingest.MetadataJob#setMetadataConverter(org.OpenGeoPortal.Ingest.Metadata.MetadataConverter)
	 */
	public void setMetadataConverter(MetadataConverter metadataConverter) {
		this.metadataConverter = metadataConverter;
	}

	/* (non-Javadoc)
	 * @see org.OpenGeoPortal.Ingest.MetadataJob#setMapServerIngest(org.OpenGeoPortal.Ingest.MapServerIngest)
	 */
	public void setMapServerIngest(MapServerIngest mapServerIngest) {
		this.mapServerIngest = mapServerIngest;
	}

	/* (non-Javadoc)
	 * @see org.OpenGeoPortal.Ingest.MetadataJob#setSolrIngest(org.OpenGeoPortal.Ingest.SolrIngest)
	 */
	public void setSolrIngest(SolrIngest solrIngest) {
		this.solrIngest = solrIngest;
	}

	/* (non-Javadoc)
	 * @see org.OpenGeoPortal.Ingest.MetadataJob#setExtraTasks(org.OpenGeoPortal.Ingest.ExtraTasks)
	 */
	public void setExtraTasks(ExtraTasks extraTasks) {
		this.extraTasks = extraTasks;
	}

	/* (non-Javadoc)
	 * @see org.OpenGeoPortal.Ingest.MetadataJob#setIngestProperties(org.OpenGeoPortal.Ingest.IngestProperties)
	 */
	public void setIngestProperties(IngestProperties ingestProperties) {
		this.ingestProperties = ingestProperties;
	}

	protected String ingestXmlMetadata(InputStream fileInputStream, String institution, String options, Object auxInfo)
			throws Exception {
				logger.info("Trying to parse metadata with stream = " + fileInputStream + ", " + institution);
				MetadataParseResponse metadataParseResponse = null;
				try {
						metadataParseResponse = metadataConverter.parse(fileInputStream, institution);
				} catch (Exception e){
					logger.info("!exception = " + e.getMessage());
					logger.info("  " + e.getCause());
					throw new Exception(e.getMessage());
				}
				logger.info("Metadata Parsed...");
			
				Metadata metadata = metadataParseResponse.metadata;
				adjustMetadata(metadata, auxInfo);
				if (!metadataParseResponse.ingestErrors.isEmpty()){
					for (IngestInfo errorObj: metadataParseResponse.ingestErrors){
						ingestStatus.addError(metadata.getOwsName(), "Parse Error: " + errorObj.getField() + "&lt;" + errorObj.getNativeName() + "&gt;:" + errorObj.getError() + "-" + errorObj.getMessage());
					}
					logger.error("Parse Errors:" + metadataParseResponse.ingestErrors.size());
				}
				if (!metadataParseResponse.ingestWarnings.isEmpty()){
					for (IngestInfo errorObj: metadataParseResponse.ingestWarnings){
						ingestStatus.addWarning(metadata.getOwsName(), "Parse Warnings: " + errorObj.getField() + "&lt;" + errorObj.getNativeName() + "&gt;:" + errorObj.getError() + "-" + errorObj.getMessage());
					}
					logger.error("Parse Warnings:" + metadataParseResponse.ingestWarnings.size());
				}
				logger.info("Metadata parsed?: " + metadataParseResponse.metadataParsed);
				Boolean doMapServerIngest = false;
				Boolean doSolrIngest = true;
				String localInstitution = ingestProperties.getProperty("local.institution");
				if (metadata.getInstitution().equalsIgnoreCase(localInstitution)){	
					doMapServerIngest  = true;
				}
				
				if (options.equalsIgnoreCase("solrOnly")){
					doMapServerIngest = false;
				} else if (options.equalsIgnoreCase("geoServerOnly")){
					doSolrIngest = false;
				}
				
				if (doMapServerIngest){
				// first update geoserver; we don't want layers in the solr index if they aren't available to the user
					logger.info("Trying map server ingest...[" + metadata.getOwsName() + "]");
					String mapServerResponse = mapServerIngest.addLayerToMapServer(localInstitution, metadata);
					//should be able to get a bbox from geoserver at this point;
					//we can update the metadata object with this new value
			
					if (mapServerResponse.toLowerCase().contains("success")){
						// store in database
						String taskResponse;
						try { 
							taskResponse =  extraTasks.doTasks(metadata);
							//this way, each institution can have it's own additional ingest actions if they desire
						} catch (Exception e) {
				
						}
					} else {
						//there was an error; return mapServerResponse as an error
						logger.error(mapServerResponse);
						ingestStatus.addError(metadata.getOwsName(), "GeoServer Error:" + mapServerResponse);
						doSolrIngest = false;
					}
				}
			
				if (doSolrIngest) {
					logger.info("Trying Solr ingest...[" + metadata.getOwsName() + "]");	
					// and ingest into solr
					SolrIngestResponse solrIngestResponse = null;
					try {				
						solrIngestResponse = solrIngest.writeToSolr(metadata, this.requiredFields);
					} catch (Exception e){ 
						ingestStatus.addError(metadata.getOwsName(), "Solr Error: " + e.getMessage());
					}
					if (!solrIngestResponse.ingestErrors.isEmpty()){
						for (IngestInfo errorObj: solrIngestResponse.ingestErrors){
							ingestStatus.addError(metadata.getOwsName(), "Solr Ingest Error: " + errorObj.getField() + "&lt;" + errorObj.getNativeName() + "&gt;:" + errorObj.getError() + "-" + errorObj.getMessage());
						}
						logger.error("Solr Ingest Errors:" + solrIngestResponse.ingestErrors.size());
					}
					if (!solrIngestResponse.ingestWarnings.isEmpty()){
						for (IngestInfo errorObj: solrIngestResponse.ingestWarnings){
							ingestStatus.addWarning(metadata.getOwsName(), "Solr Ingest Warnings: " + errorObj.getField() + "&lt;" + errorObj.getNativeName() + "&gt;:" + errorObj.getError() + "-" + errorObj.getMessage());
						}
						logger.warn("Solr Ingest Warnings:" + solrIngestResponse.ingestWarnings.size());
					}
					
					return solrIngestResponse.solrRecord.getLayerId();
				} else return "";
			}
	
	/**
	 * obtain metadata from crawl or uploading files, etc.
	 * @throws IOException
	 */
	public abstract void getMetadata() throws IOException;
	
	/**
	 * called after all data has been ingested, typically used to delete temp files
	 */
	public void cleanUp()
	{
		
	}
	
	/**
	 * subclasses can be use this to post-process metadata
	 * after it has been parsed but before it is ingested 
	 * @param metadata
	 */
	public void adjustMetadata(Metadata metadata, Object auxInfo)
	{
		
	}

	/**
	 * process passed file based on file name extension
	 * this function will handle xml and zip files
	 * @param file
	 * @param auxInfo TODO
	 */
	protected int processFile(File file, int totalFileCount, Object auxInfo)
	{
		String fileName;
		synchronized (this)
		{
			fileName = file.getName();
		}
		if (fileName.toLowerCase().endsWith(".xml"))
		{
			//treat as xml metadata
			int errorCount = ingestStatus.getErrors().size();
			processMetadataFile(file, auxInfo);
		} 
		else if (fileName.toLowerCase().endsWith(".zip"))
		{
			int zipFileCount = processZipFile(file, totalFileCount, auxInfo) - 1; 
			totalFileCount += zipFileCount;
		} else 
		{
			ingestStatus.addError(fileName, "Filetype for [" + fileName + "] is unsupported.");
		}	
		return 1;
	}
	
	
	/**
	 * called with an individual xml file to ingest
	 * it could have been uploaded, uploaded in a zip or found via a crawl
	 * @param xmlFile
	 * @param auxInfo TODO
	 */
	protected int processMetadataFile(File xmlFile, Object auxInfo)
	{
		try
		{
			String fileName;
			int errorCount = ingestStatus.getErrors().size();
			synchronized(this)
			{
				fileName = xmlFile.getName();
				ingestXmlMetadata(new FileInputStream(xmlFile), institution, options, auxInfo);
			}
			if (ingestStatus.getErrors().size() == errorCount)
				ingestStatus.addSuccess(xmlFile.getName(), "added");
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			logger.error("Failed to ingest '" + xmlFile.getName() + "'");
			String cause = "";
			if (e.getCause() == null){
				if (e.getMessage() == null)
					cause = "Unspecified error";
				else 
					cause = e.getMessage();
			}
			else 
			{
				cause = e.getCause().getClass().getName() + ":" + e.getMessage();
			}
			ingestStatus.addError(xmlFile.getName(), cause);
		}
		return 1;  // returns the number of files processed
	}
	
	/**
	 * process zip file potentially containing metadata 
	 * returns the number of files processed
	 * @param file
	 * @param auxInfo TODO
	 */
	protected int processZipFile(File file, int totalFileCount, Object auxInfo)
	{
		//first unzip the contents
		Set<File> xmlFiles = new HashSet<File>();
		try 
		{
			logger.info("Unzipping file '" + file.getName() + "'");
			xmlFiles.addAll(ZipFilePackager.unarchiveFiles(file));
		} 
		catch (Exception e) 
		{
				ingestStatus.addError(file.getName(), "Error unzipping: There is a problem with the file.");
		}
		int xmlCounter = 0;
		//totalFileCount += xmlFiles.size() - 1;
		for (File xmlFile: xmlFiles)
		{
			if (xmlFile.getName().toLowerCase().endsWith(".xml")&&(!xmlFile.getName().startsWith(".")))
			{
				xmlCounter++;
				logger.debug("Processing layer " + xmlCounter + " out of " + totalFileCount);
				ingestStatus.setProgress(xmlCounter, totalFileCount);
				processMetadataFile(xmlFile, auxInfo);
			} 
			else 
			{
				logger.info("Ignoring file: " + xmlFile.getName());
				//errorMessage.add(statusMessage(xmlFile.getName(), "Filetype is unsupported."));
			}
		}
			
		if (xmlCounter == 0)
		{
			logger.error("No XML files found in file '" + file.getName() +"'");
			ingestStatus.addError(file.getName(), "No XML files found in file");
		}
		return xmlFiles.size();
	}

		

	/* (non-Javadoc)
	 * @see org.OpenGeoPortal.Ingest.MetadataJob#run()
	 */
	public void run() 
	{
		try
		{
			getMetadata();
		} 
		catch (Exception e)
		{
			logger.error("Error in uploadMetadata");
			ingestStatus.setJobStatus(IngestJobStatus.Failed);
		}
		finally 
		{
			cleanUp();
		}
	}

}