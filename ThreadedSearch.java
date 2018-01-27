
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class ThreadedSearch {

    private static final String FILE_TO_SEARCH = "InputFileAssignment1.txt";
    private static final long FILE_SIZE_PER_THREAD = 1000; // 1k bytes per thread (Better than seperating by lines in my opinion)
    private static final String TARGET_STRING = "Glassdoor"; //I could make this like a cli which take arguments, let whoever use it to have more option

    // Entry point of the program
    public static void main(String[] args) throws Exception {
        // Open the file, calculate its total size
        RandomAccessFile file = new RandomAccessFile(FILE_TO_SEARCH, "r");
        long fileSize = new File(FILE_TO_SEARCH).length();

        // Given the file size generate the threads needed
        List<SearchThread> searchThreads = new ArrayList<>();

        if (fileSize < FILE_SIZE_PER_THREAD) {
            // If the file size is too small, we just use one thread
            searchThreads.add(new SearchThread(1, 0, fileSize));
        } else {
            // If the file size is bigger than the minimum, then okay we 
            // calculate how many threads we need
            // Calculate the partitions of each thread
            long startFilePosition = 0;

            for (int i = 0; file.getFilePointer() < fileSize; i++) {

                if (file.getFilePointer() + FILE_SIZE_PER_THREAD < fileSize) {
                    file.seek(file.getFilePointer() + FILE_SIZE_PER_THREAD);
                } else {
                    file.seek(fileSize);
                }

                file.readLine(); // Try to avoid two threads read into the same line in the text

                long endFilePosition = file.getFilePointer() - 1;
                searchThreads.add(new SearchThread(i + 1, startFilePosition, endFilePosition));

                if (file.getFilePointer() >= fileSize) {
                    break;
                }

                // Anticipate the starting point of the next thread
                startFilePosition = file.getFilePointer();
            }
        }
        
        // Start the threads
        for(SearchThread thread : searchThreads) {
            thread.start();
        }
        
        // Wait for results
        for(SearchThread thread : searchThreads) {
            thread.join();
        }
        
        // Display the results
        int totalSearchHits = 0;
        
        for(SearchThread thread : searchThreads) {
            System.out.println("Thread " + thread.getThreadID()+ " find (" + TARGET_STRING + "): " + thread.getSearchHits());
            totalSearchHits += thread.getSearchHits();
        }
        
        System.out.println("Total appearance: " + totalSearchHits);
    }

    // Runs on a separate thread. Searches a partition only
    private static class SearchThread extends Thread {

        private int threadID;
        private long startFilePosition;
        private long endFilePosition;
        private int searchHits;

        // Create a thread assigning it the partition in the file to search
        public SearchThread(int threadID, long startFilePosition, long endFilePosition) {
            this.threadID = threadID;
            this.startFilePosition = startFilePosition;
            this.endFilePosition = endFilePosition;
            searchHits = 0;
        }

        // Return the thread's unique ID for display later on
        public int getThreadID() {
            return threadID;
        }

        // Return how many of the string was found on this thread
        public int getSearchHits() {
            return searchHits;
        }

        // Start searching
        @Override
        public void run() {
            try {
                RandomAccessFile file = new RandomAccessFile(FILE_TO_SEARCH, "r");
                file.seek(startFilePosition);
                String line = file.readLine();

                // Search each line and find the string we need to search
                // keep track how many times it exists
                while (line != null) {
                    // It is possible for the target string to appear
                    // 1 or more in 1 line of string so we account for that
                    int index = 0;

                    while ((index = line.indexOf(TARGET_STRING, index)) != -1) {
                        searchHits++;
                        index += TARGET_STRING.length();
                    }

                    if (file.getFilePointer() >= endFilePosition) {
                        // We've reached the end of a partition, so we stop
                        break;
                    }

                    // Jump to the next line
                    line = file.readLine();
                }

                // Done, terminate the thread
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }
    }
}
