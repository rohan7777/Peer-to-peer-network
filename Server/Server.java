import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
    public static String formatSpecifier = "%05d";
    static final Logger logger = Logger.getLogger(Server.class.getSimpleName());
    public static final int chunkSize = 102400;// 100KB chunks.
    public static long lastChunkSize = 0;
    public static final String fileName = "test.pdf";
    private static Hashtable<String, Long> listOfChunks = null;
    static final String ServerDir = "Files/";
    static final String getTotalChunks = "getTotalChunks";
    static final String getNextChunk = "getNextChunk";
    static final String getFileName = "getFileName";
    static final String getChunkInfo = "getChunkInfo";
    static final String getLastChunkSize = "getLastChunkSize";
    static final String seperator = "#";

    public static void main(String[] args) throws Exception {
        // The server will be listening on this port number
        int serverPort = 8000;
        switch (args.length) {
            case 1:
                serverPort = Integer.parseInt(args[0]);
                break;
            default:
                serverPort = 8000;
        }
        logger.info(String.format("Input: \nServer port \t: %d", serverPort));
        fileInitilizer();
        try {
            ServerSocket serverSocket = new ServerSocket(serverPort);
            int clientNumber = 1;
            try {
                while (true) {
                    new Handler(serverSocket.accept(), clientNumber).start();
                    logger.info("Client " + clientNumber + " is connected!");
                    clientNumber++;
                }
            } finally {
                serverSocket.close();
            }
        } catch (BindException be) {
            logger.severe("Address already in use : " + serverPort + " Exiting.");
        }
    }

    private static class Handler extends Thread {
        private String requestString; // message received from the client
        private Socket socket;
        private int peerId; // The index number of the client
        int n = 150;
        public Handler(Socket connection, int no) {
            this.socket = connection;
            this.peerId = no;
        }
        @Override
        public void run() {
            try {
                // Initialize the input and output streams
                try (
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    DataOutputStream d = new DataOutputStream(out)) {
                    // Keep the connection open with the peer.
                    while (true) {
                        // receive the message sent from the client
                        requestString = ((String) in.readObject()).trim();
                        if (requestString.startsWith(getNextChunk)) {
                            int nextChunk = n;
                            int p = n % 5;
                            if (n < 0) {
                                String response = "end of chunks";
                                out.writeObject(response);
                                out.flush();
                            } else {
                                if (p == peerId || (peerId == 5 && p == 0)) {
                                    String resourceName = fileName + "." + String.format(formatSpecifier, nextChunk);
                                    String response = resourceName + seperator + listOfChunks.get(resourceName.trim());
                                    logger.info("Sending getNextChunk : " + response + " next : " + nextChunk + " size : " + listOfChunks.get(resourceName.trim()) + " has : "
                                            + listOfChunks.containsKey(resourceName.trim()));
                                    out.writeObject(response);
                                    out.flush();
                                } else {
                                    String response = "no file";
                                    out.writeObject(response);
                                    out.flush();
                                }
                            }
                            n--;
                        } else if (requestString.startsWith(getChunkInfo)) {
                            String resourceName = requestString.split(seperator)[1].trim();
                            String response = resourceName + seperator + listOfChunks.get(resourceName.trim());
                            logger.info("Sending getChunkInfo : " + response + " size " + listOfChunks.get(resourceName.trim()) + " has : " + listOfChunks.containsKey(resourceName.trim()));
                            out.writeObject(response);
                            out.flush();
                        } else if (requestString.startsWith(getTotalChunks)) {
                            out.writeObject(new Integer(listOfChunks.size()));
                            out.flush();
                        } else if (requestString.startsWith(getLastChunkSize)) {
                            out.writeObject(new Long(lastChunkSize));
                            out.flush();
                        } else if (requestString.startsWith(getFileName)) {
                            out.writeObject(fileName);
                            out.flush();
                        } else if (requestString.startsWith(fileName)) {
                            String fileName = requestString;
                            logger.info("Sending " + fileName + " from server to peer " + peerId);
                            File f = new File(ServerDir + fileName);
                            Files.copy(f.toPath(), d);
                            d.flush();
                            logger.info("Sent " + fileName + " from server to peer " + peerId);
                        }
                    }
                }
            } catch (EOFException eof) {
                logger.log(Level.SEVERE, "Peer disconnected - " + peerId);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                // Close connections
                try {
                    socket.close();
                } catch (IOException ioException) {
                    System.out.println("Disconnect with Client " + peerId);
                }
            }
        }
    }

    private static void fileInitilizer() {
        // Initialization code to read the files.
        try {
            File f = new File(ServerDir + fileName);
            int noOfChunks = (int) (f.length()/ chunkSize)+1;
            listOfChunks = new Hashtable<String, Long>();
            splitFile(f, noOfChunks);
        } catch (SecurityException | IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    /* File test.pdf will be split into test.pdf.00001, test.pdf.00002, test.pdf.00003 so on up to test.pdf.noOfChunks */
    private static void splitFile(File f, int noOfChunks) throws FileNotFoundException, IOException {
        if (f.length() > 0L) {
            int partCtr = 0;
            byte[] buffer;
            buffer = new byte[chunkSize];
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
                int tmp = 0;
                while ((tmp = bis.read(buffer)) > 0) {
                    System.out.println(tmp);
                    File newFile = new File(f.getParent(), f.getName() + "." + String.format(formatSpecifier, partCtr++));
                    try (FileOutputStream out = new FileOutputStream(newFile)) {
                        out.write(buffer, 0, tmp);
                        listOfChunks.put(newFile.getName(), newFile.length());
                        logger.info("Created File : " + newFile.getName() + " size : " + newFile.length() + "bytes");
                    }
                }
                lastChunkSize = listOfChunks.get(fileName + "." + String.format(formatSpecifier, --partCtr));
                System.out.println(listOfChunks.size());
            }
        }
    }
}

