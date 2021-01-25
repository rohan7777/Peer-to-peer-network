import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class Peer {

    static String summaryFile = "Summary.txt";
    static String peerDirPrefix = "Files/";
    static final Logger logger = Logger.getLogger(peerDirPrefix);
    static int peerPort = -1;
    static String serverHost = "localHost";
    static int serverPort = 8000;
    static int neighbourPort = -1;
    static final int chunkSize = 102400;
    static String fileName = null;
    static String formatSpecifier = "%05d";

    class Metadata {
        static final String getSummary = "getSummary";
        static final String getTotalChunks = "getTotalChunks";
        static final String getFileName = "getFileName";
        static final String getNextChunk = "getNextChunk";
        static final String getChunkInfo = "getChunkInfo";
        static final String getLastChunkSize = "getLastChunkSize";
        static final String seperator = "#";
    }

    static int totalChunks = -1;
    static Integer totalChunksCtr = -1;
    static long sizeLast = 0;
    static Long lastChunkSize = 0L;
    static Integer totalChunks2 = -1;
    static Thread downloadThread = null;
    static Thread uploadThread = null;

    public static void main(String args[]) throws Exception {

        switch (args.length) {
            case 3:
                neighbourPort = Integer.parseInt(args[2]);
            case 2:
                peerPort = Integer.parseInt(args[1]);
            case 1:
                serverPort = Integer.parseInt(args[0]);
                break;
            default:
                serverPort = 8000;
                peerPort = neighbourPort = 9999;
                System.err.println("Cannot boot with out port information. Please enter serverPort, Peer listening port & Neighbor port. Exiting.");
        }
        logger.info(String.format("Input: \nServer listening port \t: %d,\nPeer listening Port\t: %d, \nNeighbour port\t\t: %d", serverPort, peerPort, neighbourPort));
        getChunksFromServer(50);
        uploadThread = new UploadThread(peerPort);
        uploadThread.start();
        downloadThread = new DownloadThread(neighbourPort);
        downloadThread.start();
    }
    static boolean firstRun = true;
    static ArrayList<Integer> listOfChunks = null;
    synchronized static String getNextChunk() {
        if (listOfChunks != null && listOfChunks.size() > 0) {
            int index = ThreadLocalRandom.current().nextInt(0, listOfChunks.size());
            int temp = listOfChunks.get(index);
            listOfChunks.remove(index);
            return fileName + "." + String.format(formatSpecifier, temp);
        }
        return null;
    }
    private static void getChunksFromServer(int noOfChunks) throws Exception {
        logger.info("Requesting connect to " + serverHost + " at port " + serverPort);
        try (Socket requestSocket = new Socket(serverHost, serverPort);
            ObjectOutputStream outputStream = new ObjectOutputStream(requestSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(requestSocket.getInputStream());) {
            logger.info("Connected to " + serverHost + " at port " + serverPort);
            if (firstRun) {
                // Get how many total count of chunks from the server. We need to download these many chunks from server/peers.
                outputStream.writeObject(Metadata.getFileName);
                outputStream.flush();
                fileName = (String) inputStream.readObject();
                outputStream.writeObject(Metadata.getTotalChunks);
                outputStream.flush();
                totalChunks = Integer.parseInt(String.valueOf(inputStream.readObject()));
                totalChunksCtr = totalChunks;
                totalChunks2 = totalChunks;
                noOfChunks = totalChunks;
                outputStream.writeObject(Metadata.getLastChunkSize);
                outputStream.flush();
                sizeLast = Integer.parseInt(String.valueOf(inputStream.readObject()));
                lastChunkSize = sizeLast;
                logger.severe("Got total chunks from server as : " + totalChunksCtr);
                listOfChunks = new ArrayList<>();
                for (int i = 0; i < totalChunks; i++) {
                    listOfChunks.add(i);
                }
            }
            HashMap<String, Long> chunkNames = new HashMap<String, Long>();
            // Download some chunks from the server, Pass the peer id to help server differentiate amongst them.
            while (noOfChunks > 0 && listOfChunks.size() > 0) {
                System.out.println(noOfChunks);
                if (firstRun) {
                    // Get the list of files residing on the server.
                    outputStream.writeObject(Metadata.getNextChunk + Metadata.seperator + peerDirPrefix);
                    outputStream.flush();
                } else {
                    String next = getNextChunk();
                    System.out.println("Inside getNextChunk()");
                    if (next == null) {
                        mergeChunks();
                        return;
                    }
                    outputStream.writeObject(Metadata.getChunkInfo + Metadata.seperator + next);
                    outputStream.flush();
                }
                String str = (String) inputStream.readObject();
                if( str.equals("end of chunks") ) {
                    break;
                }
                if( str.equals("no file") ) {
                    noOfChunks--;
                    continue;
                }
                String[] info = str.split(Metadata.seperator);
                String chunkName = info[0];
                long size = chunkSize;
                if (chunkName != null && size > 0) {
                    File f = new File(peerDirPrefix + chunkName);
                    if (f.exists())
                        continue;
                    // Request for the above received chunk.
                    logger.info("Requesting chunk : " + chunkName);
                    outputStream.writeObject(chunkName);
                    outputStream.flush();
                    // Receive file
                    long timeStamp = System.currentTimeMillis();
                    f.getParentFile().mkdirs();
                    try (OutputStream fos = new FileOutputStream(f)) {
                        byte[] bytes = new byte[chunkSize];
                        int count;
                        int totalBytes = 0;
                        long i = 0;
                        while ((count = inputStream.read(bytes)) > 0) {
                            totalBytes += count;
                            logger.finest("Writing " + count + " total : " + totalBytes + " size : " + chunkSize);
                            fos.write(bytes, 0, count);
                            if (totalBytes == size || (f.getName().endsWith(String.valueOf(totalChunks-1)) && totalBytes >= sizeLast))
                                break;
                        }
                        System.out.println("after");
                        fos.flush();
                        logger.info("Created chunk : " + chunkName + " in " + (System.currentTimeMillis() - timeStamp));
                        listOfChunks.remove(extractId(chunkName));
                        chunkNames.put(chunkName, size);
                    }
                }
                noOfChunks--;
                System.out.println(noOfChunks);
            }
            logger.info(peerDirPrefix + " Received " + noOfChunks + " from " + serverHost);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(peerDirPrefix + summaryFile, true))) {
                for (String chunkName : chunkNames.keySet()) {
                    String entry = chunkName + Metadata.seperator + chunkNames.get(chunkName);
                    writer.write(entry);
                    writer.newLine();
                    totalChunksCtr--;
                    if (totalChunksCtr == 0)
                        mergeChunks();
                }
            }
            firstRun = false;
        } catch (ConnectException c) {
            System.err.println("Connection refused. You need to initiate a server first.");
        } catch (UnknownHostException u) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return;
    }

    static Integer extractId(String fName) {
        return fName == null ? -1 : Integer.parseInt(fName.replace(fileName + ".", ""));
    }

    synchronized private static void appendToSummary(String chunkName, Long size) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(peerDirPrefix + summaryFile, true))) {
            totalChunksCtr--;
            writer.write(chunkName + Metadata.seperator + size);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void mergeChunks() throws Exception {
        // System converged. Merge all the files and regenerate the original file.
        OutputStream fileOutputStream = new FileOutputStream(peerDirPrefix + fileName);
        for (int i = 0; i < totalChunks; i++) {
            String fname = peerDirPrefix + fileName + "." + String.format(formatSpecifier, i);
            logger.info("Looking for file " + fname);
            File f = new File(fname);
            Files.copy(f.toPath(), fileOutputStream);
        }
        fileOutputStream.flush();
        logger.info("Downloaded File!!");
        fileOutputStream.close();
        downloadThread.interrupt();
    }

    private static HashMap<String, Long> summaryAsMap() {
        File f = new File(peerDirPrefix + summaryFile);
        if (f.exists()) {
            HashMap<String, Long> list = new HashMap<String, Long>();
            try {
                try (FileInputStream fis = new FileInputStream(f); BufferedReader br = new BufferedReader(new InputStreamReader(fis));) {
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        if (line.contains(Metadata.seperator)) {
                            String[] arr = line.split(Metadata.seperator);
                            list.put(arr[0], Long.parseLong(arr[1]));
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return list;
        }
        return null;
    }

    private static class DownloadThread extends Thread {
        int neighbourPort;
        public DownloadThread(int neighbourPort) {
            this.neighbourPort = neighbourPort;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            Socket neighSocket = null;
            try {
                boolean isConnected = false;
                while (!isConnected) {
                    try {
                        neighSocket = new Socket(serverHost, neighbourPort);
                        isConnected = neighSocket.isConnected();
                    } catch (Exception e) {
                        try {
                            if (neighSocket == null || !neighSocket.isConnected()) {
                                long sleepTime = 5000;
                                logger.info("Sleeping for " + (sleepTime / 1000) + " seconds as the neighbor has not yet booted.");
                                Thread.sleep(sleepTime);
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
                int consecutiveFailureCount = 0;
                try (ObjectOutputStream out = new ObjectOutputStream(neighSocket.getOutputStream()); ObjectInputStream in = new ObjectInputStream(neighSocket.getInputStream());) {
                    while (true) {
                        logger.info("Requesting summary of " + neighSocket.toString() + " totalChunksCtr " + totalChunksCtr);
                        out.writeObject(Metadata.getSummary);
                        out.flush();
                        HashMap<String, Long> neighList = (HashMap<String, Long>) in.readObject();
                        HashMap<String, Long> diffList = getDiffOfSummary(neighList);
                        consecutiveFailureCount = 0;
                        logger.info("Received summary. Found new chunks: " + diffList);
                        // Iteratively request and receive all the files.
                        for (String chunkName : diffList.keySet()) {
                            // Request for a chunk from the above difference of summary list.
                            logger.info("Requesting chunk : " + chunkName);
                            out.writeObject(chunkName);
                            out.flush();
                            // Get the file
                            long timeStamp = System.currentTimeMillis();
                            File f = new File(peerDirPrefix + chunkName);
                            if (f.exists())
                                continue;
                            f.getParentFile().mkdirs();
                            try (OutputStream fos = new FileOutputStream(f)) {
                                byte[] bytes = new byte[chunkSize];
                                int count;
                                int totalBytes = 0;
                                while ((count = in.read(bytes)) > 0) {
                                    totalBytes += count;
                                    logger.finest("Writing " + count + " total : " + totalBytes + " size : " + chunkSize);
                                    fos.write(bytes, 0, count);
                                    if (totalBytes == diffList.get(chunkName) || (chunkName.endsWith(String.valueOf(totalChunks2 - 1)) && totalBytes == lastChunkSize))
                                        break;
                                }
                                fos.flush();
                                logger.info("Created chunk : " + chunkName + " in " + (System.currentTimeMillis() - timeStamp));
                                listOfChunks.remove(extractId(chunkName));
                                appendToSummary(chunkName, diffList.get(chunkName));
                            }
                        }
                        if (totalChunksCtr == 0) {
                            mergeChunks();
                            break;
                        } else {
                            Thread.sleep(100);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (neighSocket != null && neighSocket.isConnected())
                    try {
                        neighSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }
        }
        private HashMap<String, Long> getDiffOfSummary(HashMap<String, Long> neighList) {
            HashMap<String, Long> diff = (HashMap<String, Long>) neighList.clone();
            if (neighList != null && neighList.size() > 0) {
                HashMap<String, Long> primary = summaryAsMap();
                for (String key : primary.keySet()) {
                    diff.remove(key);
                }
            }
            return diff;
        }
    }

    private static class UploadThread extends Thread {
        int myPort = -1;
        public UploadThread(int myPort) {
            this.myPort = myPort;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(myPort);
                logger.info("Ready for uploading to the peer.");
                while (true) {
                    Socket con = serverSocket.accept();
                    try (ObjectInputStream inputStream = new ObjectInputStream(con.getInputStream());
                         ObjectOutputStream outputStream = new ObjectOutputStream(con.getOutputStream());
                         DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                        while (true) {
                            String request = (String) inputStream.readObject();
                            if (request.equals(Metadata.getSummary)) {
                                logger.info("Sending summary to " + con.toString());
                                outputStream.writeObject(summaryAsMap());
                            } else if (request.startsWith(fileName)) {
                                File f = new File(peerDirPrefix + request);
                                Files.copy(f.toPath(), dataOutputStream);
                                dataOutputStream.flush();
                                logger.info("Sent " + f.getName() + " to peer " + con.toString());
                            }
                        }
                    } catch (IOException e) {
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (serverSocket != null && !serverSocket.isClosed())
                    try {
                        serverSocket.close();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
            }
        }
    }
}