package org.OpenGeoPortal.Ingest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.OpenGeoPortal.Ingest.AbstractSolrIngest.MetadataElement;
import org.OpenGeoPortal.Ingest.IngestResponse.IngestInfo;
import org.OpenGeoPortal.Ingest.Metadata.MetadataConverter;
import org.OpenGeoPortal.Ingest.Metadata.MetadataParseResponse;
import org.OpenGeoPortal.Layer.BoundingBox;
import org.OpenGeoPortal.Layer.GeometryType;
import org.OpenGeoPortal.Layer.Metadata;
import org.OpenGeoPortal.Layer.PlaceKeywords;
import org.OpenGeoPortal.Layer.ThemeKeywords;
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

	private static final Class<? extends AbstractMetadataJob> Charset = null;
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
					
					SolrIngestResponse solrIngestResponse = solrIngestMetadata(metadata);
					// could this instead return metadata.getId()? 
					return solrIngestResponse.solrRecord.getLayerId();
				} else return "";
			}
	
	/**
	 * ingest the passed metadata into the Solr instance
	 * @param metadata
	 * @param solrIngestResponse
	 */
	protected SolrIngestResponse solrIngestMetadata(Metadata metadata)
	{
		logger.info("Trying Solr ingest...[" + metadata.getOwsName() + "]");	
		SolrIngestResponse solrIngestResponse = null;
		try {				
			solrIngestResponse = solrIngest.writeToSolr(metadata, this.requiredFields);
		} catch (Exception e){ 
			ingestStatus.addError(metadata.getOwsName(), "Solr Error: " + e.getMessage());
			logger.info("Solr ingest exception: " + e.toString());
		}
		if (!solrIngestResponse.ingestErrors.isEmpty()){
			for (IngestInfo errorObj: solrIngestResponse.ingestErrors){
				ingestStatus.addError(metadata.getOwsName(), "Solr Ingest Error: " + errorObj.getField() + "&lt;" + errorObj.getNativeName() + "&gt;:" + errorObj.getError() + "-" + errorObj.getMessage());
				logger.info("Solr ingest error: " + errorObj.getField() + "&lt;" + errorObj.getNativeName() + "&gt;:" + errorObj.getError() + "-" + errorObj.getMessage());
			}
			logger.error("Solr Ingest Errors:" + solrIngestResponse.ingestErrors.size());
		}
		if (!solrIngestResponse.ingestWarnings.isEmpty()){
			for (IngestInfo errorObj: solrIngestResponse.ingestWarnings){
				ingestStatus.addWarning(metadata.getOwsName(), "Solr Ingest Warnings: " + errorObj.getField() + "&lt;" + errorObj.getNativeName() + "&gt;:" + errorObj.getError() + "-" + errorObj.getMessage());
			}
			logger.warn("Solr Ingest Warnings:" + solrIngestResponse.ingestWarnings.size());
		}
		return solrIngestResponse;			
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
		}
		else if (fileName.toLowerCase().endsWith(".lbl") || fileName.toLowerCase().endsWith(".img"))
		{
			if (fileName.toLowerCase().contains("gazetter") == false)
				processPdsFile(file, auxInfo);
		}
		else 
		{
			ingestStatus.addError(fileName, "Filetype for [" + fileName + "] is unsupported.");
		}	
		return 1;
	}
	
	/**
	 * processes nasa planetary data in pds format
	 * should the crawl only include data with:
	 *  COORDINATE_SYSTEM_TYPE       = "BODY-FIXED ROTATING"
 	 *  COORDINATE_SYSTEM_NAME       = "PLANETOCENTRIC"
	 * @param lblFile
	 * @param auxInfo
	 * @return
	 */
	protected int processPdsFile(File lblFile, Object auxInfo)
	{
		try
		{
			Metadata metadata = new Metadata("");
	    	
			FileInputStream fileInputStream = new FileInputStream(lblFile);
			InputStreamReader foo;
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
			String pdsFileContents = "";
			String currentLine;
			double maximumLatitude = 0.0, minimumLatitude = 0.0, maximumLongitude = 0.0, minimumLongitude = 0.0;
			double centerLatitude = 0.0, centerLongitude = 0.0;
			String productId = "", dataSetId = "", planet = "", stopTime = "", producerId = "", producerInstitutionName = "";
			String instrumentName = "", targetName = "";
			String image = "";  // the filename of the actual data, relative to the url for the metadata
			currentLine = reader.readLine();
			boolean endOfHeader = false;
			while (currentLine != null)
			{
				pdsFileContents += currentLine + "\n";
				currentLine = currentLine.trim();
				logger.info(" current line = " + currentLine);
				if (currentLine.startsWith("PRODUCER_ID"))
					producerId = getPdsValue(currentLine);
				else if (currentLine.startsWith("PRODUCER_INSTITUTION_NAME"))
					producerInstitutionName = getPdsValue(currentLine);
				else if (currentLine.startsWith("MAXIMUM_LATITUDE"))
					maximumLatitude = parsePdsDouble(getPdsValue(currentLine));
				else if (currentLine.startsWith("MINIMUM_LATITUDE"))
					minimumLatitude = parsePdsDouble(getPdsValue(currentLine));
				else if (currentLine.startsWith("MAXIMUM_LONGITUDE"))
					maximumLongitude = parsePdsDouble(getPdsValue(currentLine));
				else if (currentLine.startsWith("MINIMUM_LONGITUDE"))
					minimumLongitude = parsePdsDouble(getPdsValue(currentLine));
				else if (currentLine.startsWith("EASTERNMOST_LONGITUDE"))
					maximumLongitude = parsePdsDouble(getPdsValue(currentLine));
				else if (currentLine.startsWith("WESTERNMOST_LONGITUDE"))
					minimumLongitude = parsePdsDouble(getPdsValue(currentLine));
				else if (currentLine.startsWith("CENTER_LATITUDE"))
					centerLatitude = parsePdsDouble(getPdsValue(currentLine));
				else if (currentLine.startsWith("CENTER_LONGITUDE"))
					centerLongitude = parsePdsDouble(getPdsValue(currentLine));
				else if (currentLine.startsWith("PRODUCT_ID"))
					productId = getPdsValue(currentLine);
				else if (currentLine.startsWith("DATA_SET_ID"))
					dataSetId = getPdsValue(currentLine);
				else if (currentLine.startsWith("TARGET_NAME"))
					planet = getPdsValue(currentLine);
				else if (currentLine.startsWith("STOP_TIME"))
					stopTime = getPdsValue(currentLine);
				else if (currentLine.startsWith("^IMAGE"))
					image = getPdsValue(currentLine);
				else if (currentLine.startsWith("INSTRUMENT_NAME"))
					instrumentName = getPdsValue(currentLine);
				else if (currentLine.startsWith("TARGET_NAME"))
					targetName = getPdsValue(currentLine);
				else if (currentLine.equals("END"))
				{
					endOfHeader = true;  // important when using embedded header
					logger.info("  END reached");
				}
				if (endOfHeader == false)
					currentLine = reader.readLine();
				else
					currentLine = null;
			}
			
			logger.info("minimumLatitude = " + minimumLatitude + ", maximumLatitude = " + maximumLatitude);
			logger.info("minimumLongitude = " + minimumLongitude + ", maximumLongitude = " + maximumLongitude);
			logger.info("centerLatitude = " + centerLatitude + ", centerLongitude = " + centerLongitude);
			if (minimumLatitude == 0.0 && maximumLatitude == 0.0 && centerLatitude == 0.0 && centerLongitude == 0.0)
			{
				// here if there probably was any location specified in the metadata
				logger.info("no location information found, abortng ingest");
				return 1;
			}
			if (centerLongitude > 180)
				centerLongitude = 360. - centerLongitude;
			if (minimumLongitude > 180)
				minimumLongitude = 360. - minimumLongitude;
			if (maximumLongitude > 180)
				maximumLongitude = 360. - maximumLongitude;
			if (maximumLatitude == 0.0 && minimumLatitude == 0.0)
			{
				// here if the metadata specified center lat/lon rather then min/max lat/lon
				// we just assume a very small area for any sensor's bounding box
				// ideally, we could compute something based on sensor parameters
				// but is NASA hasn't done that, I'm not sure we can do it here
				//  it might be available from resolution and rows/columns
				double epsilon = .01;
				minimumLatitude = centerLatitude - epsilon;
				maximumLatitude = centerLatitude + epsilon;
				minimumLongitude = centerLongitude - epsilon;
				maximumLongitude = centerLongitude + epsilon;
			}
			logger.info("minimumLatitude = " + minimumLatitude + ", maximumLatitude = " + maximumLatitude);
			logger.info("minimumLongitude = " + minimumLongitude + ", maximumLongitude = " + maximumLongitude);
			BoundingBox boundingBox = new BoundingBox(minimumLongitude, minimumLatitude, maximumLongitude, maximumLatitude);
			metadata.setBounds(boundingBox);
			metadata.setFullText(pdsFileContents);
			String title = createPdsTitle(productId, dataSetId);
			metadata.setTitle(title);
			metadata.setId(title.replace(" ", "-"));
			metadata.setOwsName(title);
			if (stopTime.length() > 3)
				stopTime = stopTime.substring(0,3);
			if (producerId == "")
				metadata.setOriginator(producerInstitutionName);
			else
				metadata.setOriginator(producerId);
			
			metadata.setContentDate(stopTime);
			metadata.setGeometryType(GeometryType.Raster);
			metadata.setInstitution("NASA");
			List<ThemeKeywords> themeKeywordList = new ArrayList<ThemeKeywords>();
			metadata.setThemeKeywords(themeKeywordList);
			ThemeKeywords themes = new ThemeKeywords();
			themes.addKeyword(instrumentName);
			themes.addKeyword(producerInstitutionName);
			themeKeywordList.add(themes);
			List<PlaceKeywords> placeKeywordList = new ArrayList<PlaceKeywords>();
			metadata.setPlaceKeywords(placeKeywordList);
			PlaceKeywords places = new PlaceKeywords();
			places.addKeyword(targetName);
			placeKeywordList.add(places);
			logger.info("created metadata from PDS " + metadata.getTitle());
			logger.info("  targetName = " + targetName);
			SolrIngestResponse solrIngestResponse = solrIngestMetadata(metadata);
		} 
		catch (FileNotFoundException e)
		{
			logger.error("did not find PDS file " + e);
		}
		catch (IOException e) 
		{
			logger.error("IO exception for PDS file " + e);
		}
		
		return 1;
	}
	
	protected double parsePdsDouble(String pdsValue)
	{
		if (pdsValue.contains(" "))
		{
			String[] parts = pdsValue.split(" ");
			try
			{
				double value = Double.parseDouble(parts[0]);
				return value;
			}
			catch (java.lang.NumberFormatException e)
			{
				logger.error("in AbstractMetadataJob.parsePdsDouble, could not parse " + pdsValue + ", " + e.toString());
				return 0;
			}
		}
		return Double.parseDouble(pdsValue);
	}
	
	protected String createPdsTitle(String productId, String dataSetId)
	{
		logger.info("productId = " + productId);
		if (productId.contains("\""))
		{
			productId = productId.replace("\"", "");
			logger.info("  contains quote: " + productId);
		}
		if (dataSetId.contains("\""))
			dataSetId = dataSetId.replace("\"", "");
		if (productId.endsWith(".img") || productId.endsWith(".IMG"))
			productId = productId.substring(0, productId.lastIndexOf("."));
		return productId + "  " + dataSetId;
	}
	protected String getPdsValue(String line)
	{
		if (line.contains("=") == false)
			return "";
		int index = line.indexOf("=");
		String returnValue = line.substring(index + 1);
		if (returnValue.contains("\""))
			returnValue.replace("\"", "");
		returnValue = returnValue.trim();
		return returnValue;
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
			xmlFile.delete();
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