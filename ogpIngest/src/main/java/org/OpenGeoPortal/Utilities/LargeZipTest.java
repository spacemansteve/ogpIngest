package org.OpenGeoPortal.Utilities;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

public class LargeZipTest 
{

	public static void main(String[] args)
	{
		System.out.println("top of LargeZipFile");
		try 
		{
			String url$ = "http://cod.humanitarianresponse.info/sites/cod.humanitarianresponse.info/files/ben_roads.zip";
			URL url = new URL(url$);
			InputStream stream = url.openStream();
			int available = stream.available();
			byte[] bytes = new byte[available];
			stream.read(bytes);
			System.out.println("available = " + stream.available());
			for (int i = 0 ; i < bytes.length ; i++)
				System.out.print(bytes[i] + " " );
			//ZipInputStream zipStream = new ZipInputStream(stream);
			Thread.sleep(4);
			//ZipEntry entry = zipStream.getNextEntry();
			//ZipArchiveInputStream zipStream = new ZipArchiveInputStream(stream);
			
			//System.out.println("zipStream = " + zipStream.toString());
			//System.out.println(zipStream.available());

			//ZipArchiveEntry entry = zipStream.getNextZipEntry();
			//System.out.println("  entry = " + entry);
			//while (entry != null)
			//{
				//String name = entry.getName();
				//System.out.println(name);
				//entry = zipStream.getNextEntry();
			//}
		} 
		catch (MalformedURLException e) 
		{
			e.printStackTrace();
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		System.out.println(" end of LargeZipTest");
		
	}
}
