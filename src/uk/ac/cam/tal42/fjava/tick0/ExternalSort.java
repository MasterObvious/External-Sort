package uk.ac.cam.tal42.fjava.tick0;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.PriorityQueue;

public class ExternalSort {
	
	private static final long MAX_MEMORY = 3100000;

	public static void sort(String f1, String f2) throws FileNotFoundException, IOException {
		//create a random access file for file A, a data input stream and an output stream
		RandomAccessFile fileA = new RandomAccessFile(f1, "rw");
		DataInputStream fisA = new DataInputStream(new BufferedInputStream(new FileInputStream(fileA.getFD())));
		DataOutputStream fosA = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileA.getFD())));
		


		//store the file size
		long fileSize = fileA.length();

		//can we sort the whole file in one go?
		if (fileSize < MAX_MEMORY) {
			//if there's more than 1 number we need to sort
			if (fileSize > 4) {
				//create a byte array
				byte[] byteArray = new byte[(int) fileSize];
				//read into the byte array
				fisA.read(byteArray);
				//convert to an array of ints
				//int[] intArray = toIntArray(byteArray);
				//sort 
				twoByteSort(byteArray);
				//set the length of file A to 0
				fileA.setLength(0);
				//write the int array as a byte array to the output stream
				fosA.write(byteArray);
				//flush
				fosA.flush();
				//Done
			}
		}else {
			//create a random access file for file B and a data output stream
			RandomAccessFile fileB = new RandomAccessFile(f2, "rw");
			DataOutputStream fosB = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileB.getFD())));
			//STAGE 1: create chunks of sorted numbers
			fileB.setLength(0);
			//initialise a file pointer
			long filePointer = 0;
			//and the number of chunks
			int numberOfChunks = 0;
			byte[] byteArray = new byte[(int) MAX_MEMORY];
			//while we haven't reached the end of the file
			while(filePointer < fileSize) {
				//for each chunk calculate the number of bytes to read
				int bytesToRead = (int) Math.min(MAX_MEMORY, fileSize - filePointer);
				//create an array of that size
				if (bytesToRead != byteArray.length) {
					byteArray = new byte[bytesToRead];
				}
				//read bytes into the byte array
				fisA.read(byteArray);
				twoByteSort(byteArray);
				
				//convert to an int array
				//int[] intArray = toIntArray(byteArray);
				//sort 
				//Arrays.sort(intArray);
				//write the int array as a byte array to the output stream
				fosB.write(byteArray);
				//and flush
				fosB.flush();
				//increment the file pointer
				filePointer += MAX_MEMORY;
				//and number of chunks
				numberOfChunks += 1;
			}
			//STAGE 2: merge these chunks
			
			//empty file A
			fileA.setLength(0);
			//create pointer and inputstream
			RandomAccessFile file;
			DataInputStream inputStream;
			//create a priority queue
			PriorityQueue<ChunkHead> pq = new PriorityQueue<>();
			
			
			for (int i = 0; i < numberOfChunks; i += 1 ) {
				//create a random access file
				file = new RandomAccessFile(f2, "rw");
				//seek to the right position
				long pos = MAX_MEMORY * i;
				file.seek(pos);
				//create an input stream for this file pointer
				inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file.getFD())));
				//set up the chunk head
				pq.add(new ChunkHead(inputStream.readInt(), inputStream, (int) Math.min(MAX_MEMORY, file.length() - pos)));
			}
			
			//while there's something in the priority queue
			while(!pq.isEmpty()) {
				//get the chunk head at the front of the queue
				ChunkHead min = pq.poll();
				//write the int to the file
				fosA.writeInt(min.minVal);
				//decrement the counter
				min.bytesRemaining -= 4;
				//if we can read in another int and add to the queue
				if (min.bytesRemaining > 0) {
					min.minVal = min.chunkStream.readInt();
					pq.add(min);
				}
			}
			fosA.flush();	
			fosB.close();
			fileB.close();
		}
		
		fisA.close();
		fosA.close();
		fileA.close();
		
		
	}
	
	private static byte[] sort(byte[] input){
		byte[] output = new byte[input.length];
		int[] count = new int[256];
		
		//do first 3 bytes as unsigned
		for(int digit = 3; digit>0; digit-=1) {
			Arrays.fill(count, 0);
			//count digits
			for(int i = 0; i < input.length; i+=4) {
				count[input[i + digit]&0xFF] += 4;
			}
			//make it cumulative
			for (int i = 1; i < count.length; i+=1) {
				count[i] += count[i-1];
			}
			//put it into the output array
			for (int i = input.length-4; i >=0; i-=4) {
				int index = input[i + digit]&0xFF;
				
				count[index] = count[index] - 4;
				int pos = count[index];
				output[pos] = input[i];
				output[pos + 1] = input[i+1];
				output[pos + 2] = input[i+2];
				output[pos + 3] = input[i+3];
				
			}
			
			byte[] temp = input;
			input = output;
			output = temp;
		}
		
		//last byte needs to be treated negatively
		Arrays.fill(count, 0);
		//count digits
		for(int i = 0; i < input.length; i+=4) {
			count[input[i]&0xFF] += 4;
		}
		//so cumulative is different as negative numbers come first
		for (int i = 129; i < count.length; i+=1) {
			count[i] += count[i-1];
		}
		count[0] += count[count.length-1];
		for (int i = 1; i < 128; i+=1) {
			count[i] += count[i-1];
		}
		
		//put it into the output array
		for (int i = input.length-4; i >=0; i-=4) {
			int index = input[i]&0xFF;
			
			count[index] = count[index] - 4;
			int pos = count[index];
			output[pos] = input[i];
			output[pos + 1] = input[i+1];
			output[pos + 2] = input[i+2];
			output[pos + 3] = input[i+3];
			
		}
		
		return output;
		
		
	}
	
	private static void twoByteSort(byte[] input) {
		byte[] output = new byte[input.length];
		int[] count = new int[65536];
		
		//first 2 bytes treated absolutely
		//first pass counting
		for (int i = 0; i < input.length; i += 4) {
			count[((input[i + 2] & 0xFF) << 8) | (input[i + 3] & 0xFF)] += 4;
		}
		//make it cumulative
		for (int i = 1; i < count.length; i += 1) {
			count[i] += count[i-1];
		}
		//put into the output array
		int index = 0;
		int pos = 0;
		for (int i = input.length - 4; i>=0; i-=4) {
			index = ((input[i + 2] & 0xFF) << 8) | (input[i + 3] & 0xFF);
			count[index] = count[index] - 4;
			pos = count[index];
			output[pos] = input[i];
			output[pos + 1] = input[i+1];
			output[pos + 2] = input[i+2];
			output[pos + 3] = input[i+3];
		}
		//empty the count
		Arrays.fill(count, 0);
		
		//second pass uses a different cumalitive method as negative arrays
		for (int i = 0; i < input.length; i += 4) {
			count[((output[i] & 0xFF) << 8) | (output[i + 1] & 0xFF)] += 4;
		}
		//make it cumulative but take negative first
		for (int i = 32769; i < count.length; i += 1) {
			count[i] += count[i-1];
		}
		count[0] += count[65535];
		for (int i = 1; i < 32768; i+=1) {
			count[i] += count[i-1];
		}
		//put into the output array
		for (int i = input.length - 4; i>=0; i-=4) {
			index = ((output[i] & 0xFF) << 8) | (output[i + 1] & 0xFF);
			count[index] = count[index] - 4;
			pos = count[index];
			input[pos] = output[i];
			input[pos + 1] = output[i+1];
			input[pos + 2] = output[i+2];
			input[pos + 3] = output[i+3];
		}
		
	}
	
	
	private static class ChunkHead implements Comparable<ChunkHead> {
		private int minVal;
		DataInputStream chunkStream;
		private int bytesRemaining;
		
		public ChunkHead(int mV, DataInputStream cS, int bR) {
			minVal = mV;
			chunkStream = cS;
			bytesRemaining = bR;
		}
		
		@Override
		public int compareTo(ChunkHead arg0) {
			// TODO Auto-generated method stub
			return Integer.compare(this.minVal, arg0.minVal);
		}
		
	}
	

	private static String byteToHex(byte b) {
		String r = Integer.toHexString(b);
		if (r.length() == 8) {
			return r.substring(6);
		}
		return r;
	}

	public static String checkSum(String f) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			DigestInputStream ds = new DigestInputStream(
					new FileInputStream(f), md);
			byte[] b = new byte[512];
			while (ds.read(b) != -1)
				;

			String computed = "";
			for(byte v : md.digest()) 
				computed += byteToHex(v);

			return computed;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "<error computing checksum>";
	}

	public static void main(String[] args) throws Exception {
		String f1 = args[0];
		String f2 = args[1];
		long startTime = System.nanoTime();
		sort(f1, f2);
		System.out.println("It took " + (System.nanoTime() - startTime));
		
		System.out.println("The checksum is: "+checkSum(f1));
	}
}
