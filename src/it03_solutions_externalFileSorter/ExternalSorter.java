package it03_solutions_externalFileSorter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/*  

*/

/**
 * ExternalSorter
 	Sorts an input file and writes to an output file using an external sorting algorithm.  
 	
 	TL;DR
	Big Unsorted file -> split into smaller files -> recursively sort/merge smaller files
	
 	External sorting is a class of sorting algorithms that can handle massive amounts of data. Required when
	data do not fit into memory of device, must reside in slower memory, ie disk. Merge sort uses a hybrid approach 
	where data is 'chunked' into pieces which fit into memory are read, sorted, and written to a temp file. Afterwards, 
	they are merged into a final file. 
	
 * @author jstrand
 * 
 *
 */
public class ExternalSorter {
	// TODO Look into how to use dynamic system values as constraints on file size chunks - would be valuable if running as a multi-threaded program.
	// THese can be set as the xmax and xmin to the JVM at run time. 
	// Get current size of heap in bytes
	private static long heapSize = Runtime.getRuntime().totalMemory(); 
	// Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
	private static long heapMaxSize = Runtime.getRuntime().maxMemory();
	 // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
	private static  long heapFreeSize = Runtime.getRuntime().freeMemory(); 
	
	
	private static Integer maxBufferSize = 50; 
	private static Integer maxFileSize = 100000;
	private static String userMessage = "Your file sort is successful, please check results at: ";
	private static String delimit = "//";
	private final static String windowsDelimit = "\\";
	
	
	public static String externalSort(String sortDir, String sortFile, String sortTempDir, String sortedFileName) {
		
	    userMessage =  userMessage + sortDir + delimit;
	    
		try {
			// Try to get environment variables set for max buffer and max file size.
			// If they are null or empty use default, else parse int. If this fails send error message to user. 
			String envMaxBufferSize = System.getenv("EX_SORT_MAX_BUFFER");
	        String envMaxFileSize = System.getenv("EX_SORT_MAX_FILE");
	        maxBufferSize = (envMaxBufferSize == null || envMaxBufferSize.isEmpty())  ? maxBufferSize : Integer.parseInt(envMaxFileSize);
	        maxFileSize = ( envMaxFileSize == null || envMaxFileSize.isEmpty()) ? maxFileSize : Integer.parseInt(envMaxFileSize); 
			
	        // Try to get the os type to set filesystem delimiter
	        String os = System.getProperty("os.name");
	        delimit = (os == null || os.isEmpty() || os == "windows" ) ? windowsDelimit : delimit; 
	        
	        // Instantiate new Comparator object, which defines how the strings are compared. 
			ExternalSortCompare fileSortComp = new ExternalSortCompare();
			
			// Create the tempDir, where input file will be split for sorting.
			String tempDir = getTempDir(sortTempDir);
						
			File checkSortDirExists = new File(sortDir);
			File checkSortFileExists = new File(sortDir + delimit + sortFile);
			File checkSortTempDirExists = new File(sortTempDir);

			if (!checkSortDirExists.exists() || !checkSortDirExists.isDirectory() ) {
				userMessage = "The sortDir does not exist or is not a directory: " + sortDir;
			} else if (!checkSortFileExists.exists() || checkSortFileExists.isDirectory() ) {
				userMessage = "The sortFile does not exist or is a directory: " + sortFile;
			} else if (checkSortFileExists.length() > maxFileSize ) {
				 userMessage = "File size exceeds maximum: " + sortFile + ", max file size: " + maxFileSize;
			} else if (checkSortFileExists.length() == 0 ) {
			 userMessage = "File is Empty: " + sortFile + ", max file size: " + maxFileSize;
			}
			else if (!checkSortTempDirExists.exists() || !checkSortTempDirExists.isDirectory() ) {
				userMessage = "The sortTempDir does not exist or is not a directory: " + sortTempDir;
			} else {
				// Call the splitter and the Merger functions. 
				List<File> reducedFiles = externalFileSortSplitter(sortDir, sortFile, tempDir, fileSortComp);
				File sortedFile = externalFileSortMerger(reducedFiles, sortDir, sortedFileName, fileSortComp);
				userMessage = userMessage + sortedFile;	
			}
		} 
		catch (NullPointerException e) {
			e.printStackTrace();
			userMessage = "Error occured, please contact NFIS for more info " + e;
		}
		catch (SecurityException e) {
			e.printStackTrace();
			userMessage = "Error occured accessing filesystem, please contact NFIS for more info " + e;
		}
		catch (IOException e) {
			e.printStackTrace();
			userMessage = "Error occured accessing filesystem, please contact NFIS for more info " + e;
		}
		catch (Exception e) {
			e.printStackTrace();
			userMessage = "Error occured assessing file, please contact NFIS for more info " + e;
		}
		return userMessage;
	}
	
	/*
	 * getTempDir
	 * Given a filesystem path, will create a directory with UUID in name to prevent namespace collisions
	 * 
	 * @param String tempDir Filesystem pathway at which to create temporary sort dir 
	 * @returns String Temporary sort dir pathway
	 * */
	public static String getTempDir(String tempDir) {
		String tempDirPath = tempDir + "_external_sorter_" + UUID.randomUUID(); 
		File td = new File(tempDirPath);
		if (!td.exists()) {
			td.mkdir();
		}
		td.deleteOnExit();
		return tempDirPath;
	}
	
	/*
	 * externalFileSortSplitter
	 * Given a pathway to file for sorting, streams file into memory in chunks that fit in buffer size, sorts, and writes to a temporary file. 
	 * 
	 * @param String sortDir Filesystem pathway at which file to sort is located
	 * @param String sortName Name of  file to sort 
	 * @param ExternalSortCompare fileSortComp Comparator object to use for sorting file
	 * @returns List<File> List of File objects that have been sorted to external memory.
	 * */		
	public static List<File> externalFileSortSplitter(String sortDir, String sortName, String sortTempDir, ExternalSortCompare fileSortComp) throws Exception{

		String sortPath = sortDir + delimit + sortName;
		
		// Collect temporary Files into List array for reference in merge phase
		List<File> tempFileList = new ArrayList<File>();
		
		// File-Access Buffers
		BufferedReader buffr = null;
		BufferedWriter buffw = null;
		
		// Collect lines to List Array, this array is cleared once the max Sorting buffer size is reached
		List<String> currSortLines = new ArrayList<String>();

		// Get currLine from the file
		String currLine = null;
		// Store current size of buffer
		int currSortBuffSize = 0;
		int sortFileCount = 0;

		try{ 
			// Create new buffered reader within safety of try/catch	
			buffr = new BufferedReader(new FileReader(sortPath));
			
			currLine = buffr.readLine();
			
			while ( currLine != null ){
				
				// Add current line to the line buffer, get size of line buffer in bytes
				currSortLines.add(currLine);
				currSortBuffSize += currLine.length() + 1;

				
				// Does the currSortBuffSize (number of bytes in memory) exceed our sortBufferSize?
				// TODO will need to refine the check of buffer sizes - currently will allow the currSortBuffSize to exceed
				// sortBuffSize by 1 line which could be problematic if data is very wide
				if ( currSortBuffSize >= maxBufferSize ){
					
 					// Sort the lines based on substring 0 - 1, write to file, remember the file object
					Collections.sort(currSortLines, fileSortComp);
					File tf = new File(sortTempDir + delimit + sortName + "_temp_" + sortFileCount + "_" + UUID.randomUUID() + ".txt");
					// Temporary files should be deleted when the JVM terminates.
					tf.deleteOnExit();
					
					//Write out lines to the temp file, flush and close stream
					buffw = new BufferedWriter( new OutputStreamWriter( new FileOutputStream(tf)));
					for ( int i = 0; i < currSortLines.size(); i++ ) {
						buffw.write( currSortLines.get(i) + "\n" );
					}
					
					buffw.flush();
					if (buffw != null) buffw.close();
					
					// Add this temp file to the list, reset counters
					tempFileList.add(tf);
					currSortLines.clear();
					currSortBuffSize = 0;
					sortFileCount++;

				}
				currLine = buffr.readLine();
			}

	}catch(IOException io){ 
		throw io;
	}finally{
			if ( buffr != null ) {
				try{
					buffr.close();
				}catch(Exception e){
					throw  e;
				}
			}
		}
		return tempFileList;

	}	
	
	/*
	 * externalFileSortMerger
	 * Given an array of File objects, merges contents of file to a sorted list and writes to new file
	 * 
	 * @param List<File> filesToMergeSort List of Files in which results have been sorted
	 * @param String outputDir Directory to write sorted file to
	 * @param ExternalSortCompare fileSortComp Comparator object to use for sorting file
	 * @returns File File with sorted results
	 * */		
	public static File externalFileSortMerger(List<File> filesToMergeSort, String outputDir, String outputFile, ExternalSortCompare fileSortComp ) throws IOException {
		List<File> mergeSortFiles = filesToMergeSort;		

		// TODO: it would seem that this approach simply reads all the data back into memory to sort. 
		// Each temp file would also equal the max memory.
		// Would be better to merge files line by line until all lines are sorted
		List<ExternalSortTempFile> lineBuffrMap =  new ArrayList<>();
		BufferedWriter buffw =null;
		File sortedFile = null;
		
		try{
			
			sortedFile = new File(outputDir + delimit + outputFile);
			
			buffw  = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sortedFile)));
			
			// Open a buffer for each file to process in the merge sort
			for ( int i = 0; i < mergeSortFiles.size(); i++ ){
				//Instantiate a buffer reader for each file to sort
				BufferedReader buffr = new BufferedReader(new FileReader(mergeSortFiles.get(i)));
				String buffrLine = buffr.readLine();

				while (buffrLine != null ) {
					lineBuffrMap.add( new ExternalSortTempFile( buffrLine, buffr));
					buffrLine = buffr.readLine();
				}
				
				 				
			}
			
			lineBuffrMap.sort(Comparator.comparing(ExternalSortTempFile::getSortKey));

			while ( !lineBuffrMap.isEmpty() ){
				ExternalSortTempFile lineToWrite = lineBuffrMap.remove(0) ;
				buffw.write(lineToWrite.getLine() + "\n");
				lineToWrite.closeBuffer();
			}

		}catch(IOException io){

			throw io;

		}finally{
			// Close the buffers
			if (buffw != null) buffw.close();
		}
		return sortedFile;		
	}
		
	
	public static void main (String Args[]) {
		//TODO this is a naive approach to parsing arguments, need to use flags and an options interface. 
		String sd = Args[0];
		String sf = Args[1];
		String td = Args[2];
		String std = Args[3];		
        	
		String result = externalSort(sd, sf, td, std);
		System.out.println(result);
	}
}

