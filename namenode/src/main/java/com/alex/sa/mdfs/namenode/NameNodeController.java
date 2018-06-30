package com.alex.sa.mdfs.namenode;

import com.netflix.appinfo.InstanceInfo;
import javafx.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.netflix.eureka.server.event.*;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@RestController
public class NameNodeController {
    // block
    @Value(value="${block.default-size}")
    public int BLOCK_SIZE;
    @Value(value="${block.default-replicas}")
    public int REPLICA_NUM;

    // load-balancer
    @Value(value="${load-balancer.num-visual-node}")
    public int VISUAL_NODE_NUM;

    @Value(value="${test-mode}")
    public boolean TEST_MODE;

    @Autowired
    private HttpServletRequest request;

    private DataNodeManager dataNodeManager = new DataNodeManager();
    private FileBlockManager fileBlockManager = new FileBlockManager();
    private FileTreeManager fileTreeManager = new FileTreeManager();
    private LoadBalanceManager loadBalanceManager = new LoadBalanceManager(31);

    private static String blockFileDir = "tmp/fileBlocks/";
    private static String downloadedFileDir = "tmp/downloadedFiles/";


    @GetMapping("/fileOnNodes")
    public Map<String, List<String>> listFileOnNodes() {
        return fileBlockManager.listAll();
    }

    @GetMapping("/allFiles")
    public Map<String, Long> listFileSystem() {
        return fileTreeManager.list();
    }

    @GetMapping("/**")
    @ResponseBody
    public ResponseEntity<Resource> serveFile() {
        try {
            String fileNameWithPath = request.getRequestURI().replaceFirst("/", "");
            String filename = fileNameWithPath.replace("/", "_");
            // save downloaded file in a temp path
            File downloadFile = new File(downloadedFileDir + filename);
            downloadFile.createNewFile();
            // use FileChannel to get a joint file
            FileChannel downloadFileChannel = new FileOutputStream(downloadFile).getChannel();

            // get file blocks and append them to the downloaded file
            long numBlocks = fileTreeManager.getNumBlocks(fileNameWithPath);
            for (int blockIndex = 0; blockIndex < numBlocks; blockIndex ++) {
                List<String> dataNodeURLS = fileBlockManager.getDataNodeURL(filename, blockIndex);
                String blockedFileName = blockedFileName(filename, blockIndex);
                for (String dataNodeURL : dataNodeURLS) {
                    if (dataNodeManager.isValid(dataNodeURL)) {
                        File slicedFile = getFileBlock(dataNodeURL, blockedFileName);
                        FileChannel inputChannel = new FileInputStream(slicedFile).getChannel();
                        inputChannel.transferTo(0, slicedFile.length(), downloadFileChannel);
                        inputChannel.close();
                        break;
                    }
                }
            }

            downloadFileChannel.close();
            Resource fileResource = new UrlResource(downloadFile.toURI());
            return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION,
                    "attachment; filename=\"" + fileResource.getFilename() + "\"").body(fileResource);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.notFound().build();
        }

    }

    @PutMapping("/**")
    public String handleFileUpload(@RequestParam("file") MultipartFile file) {
        try {
            String dirPath = request.getRequestURI().replaceFirst("/", "");
            String fileNameWithPath = dirPath + file.getOriginalFilename();
            String fileName = dirPath.replace('/', '_') + file.getOriginalFilename();
            if (fileTreeManager.contain(fileNameWithPath)) {
                System.err.println("File already exists.");
                return "File already exists.";
            }
            // divide into blocks
            long numBytes = file.getSize();
            long numBlocks = numBytes / BLOCK_SIZE + ((numBytes) % this.BLOCK_SIZE == 0 ? 0 : 1);

            FileInputStream fis = (FileInputStream) file.getInputStream();
            BufferedInputStream bis = new BufferedInputStream(fis);

            for(int blockIndex = 0; blockIndex < numBlocks; blockIndex ++) {
                byte blockByteArray[] = new byte[BLOCK_SIZE];
                int blockSize = bis.read(blockByteArray);
                File slicedFile = writeBlockedFile(fileName, blockByteArray, blockIndex, blockSize);

                List<String> targetDataNodes = getTargetDataNodes(slicedFile);
                for (String URL : targetDataNodes) {
                    boolean response = sendFileBlock(URL, slicedFile);
                    System.out.println("block " + blockIndex + " to data node at " + URL + " : " + response);
                    dataNodeManager.addFileBlock(URL, fileName, blockIndex);
                    fileBlockManager.addFileBlock(URL, fileName, blockIndex);
                }

                slicedFile.delete();
            }
            fileTreeManager.addFile(fileNameWithPath, numBlocks, numBytes);
            return "success";
        } catch (IOException e) {
            e.printStackTrace();
            return "fail";
        }
    }

    private List<String> getTargetDataNodes(File file) {
        return loadBalanceManager.getTargetDataNodeURLS(file, REPLICA_NUM);
//        return dataNodeManager.getAllDataNodeURL();
    }

    private void askNodeToDeleteFileBlock(String dataNodeURL, String fileName) {
        String URL = dataNodeURL + "files/" + fileName;

        // send delete request
        RestTemplate rest = new RestTemplate();
        rest.delete(URL);
    }
    private boolean sendFileBlock(String URL, File file) {
        // prepare params
        FileSystemResource resource = new FileSystemResource(file);
        MultiValueMap<String, Object> parameters = new LinkedMultiValueMap<>();
        parameters.add("file", resource);

        // send post request
        RestTemplate rest = new RestTemplate();
        String response = rest.postForObject(URL, parameters, String.class);

        return response.equals("success");
    }

    private File getFileBlock(String dataNodeURL, String fileName) {
        try {
            // down load file into a input stream
            String resourceURL = dataNodeURL + "files/" + fileName;
            UrlResource urlResource = new UrlResource(new URL(resourceURL));
            InputStream inputStream = urlResource.getInputStream();

            // write into a byte array
            byte[] bytes = new byte[BLOCK_SIZE];
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int n, blockSize = 0;
            while ( (n=inputStream.read(bytes)) != -1) {
                out.write(bytes,0,n);
                blockSize += n;
            }

            bytes = out.toByteArray();
            // write into a file
            File file = writeFile(fileName, bytes, blockSize);

            return file;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    private String blockedFileName(String fileName, long blockIndex) {
        return blockIndex + "_" + fileName;
//        return fileName + "#" + blockIndex;
    }
    private Pair<String, Long> parseBlockedFileName(String blockedFileName) {
//        int splitIndex = blockedFileName.lastIndexOf('#');
//        String fileName = blockedFileName.substring(splitIndex + 1, blockedFileName.length() - 1);
//        Long blockIndex = Long.parseLong(blockedFileName.substring(0, splitIndex - 1));
//        return new Pair<>(fileName, blockIndex);
        int splitIndex = blockedFileName.indexOf('_');
        String fileName = blockedFileName.substring(splitIndex + 1, blockedFileName.length());
        Long blockIndex = Long.parseLong(blockedFileName.substring(0, splitIndex));
        return new Pair<>(fileName, blockIndex);
    }
    private File writeFile(String fileName, byte[] blockByteArray, int length) {
        try {
            File file = new File(blockFileDir + fileName);
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(blockByteArray, 0, length);
            fos.close();
            return file;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    private File writeBlockedFile(String fileName, byte[] blockByteArray, long blockIndex, int blockSize) {
        try {
            String blockFileName = blockFileDir + blockedFileName(fileName, blockIndex);
            File slicedFile = new File(blockFileName);
            FileOutputStream fos = new FileOutputStream(slicedFile);
            fos.write(blockByteArray, 0, blockSize);
            fos.close();
            return slicedFile;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @DeleteMapping("/**")
    @ResponseBody
    public String deleteFile() {
        String fileNameWithPath = request.getRequestURI().replaceFirst("/", "");
        String filename = fileNameWithPath.replace("/", "_");
        if (!fileTreeManager.contain(fileNameWithPath)) {
            return "File doesn't exist";
        }
        long numBlocks = fileTreeManager.getNumBlocks(fileNameWithPath);
        for (int blockIndex = 0; blockIndex < numBlocks; blockIndex ++) {
            List<String> dataNodeURLS = fileBlockManager.getDataNodeURL(filename, blockIndex);
            for (String dataNodeURL : dataNodeURLS) {
                String blockedFileName = blockedFileName(filename, blockIndex);

                askNodeToDeleteFileBlock(dataNodeURL, blockedFileName);

                System.out.println("block " + blockIndex + " of file " + filename  + "on data node at " + dataNodeURL + " : deleted." );
            }
            dataNodeManager.removeBlock(filename, blockIndex);
            fileBlockManager.removeFileBlock(filename, blockIndex);
            loadBalanceManager.removeFile(blockedFileName(filename, blockIndex));
        }
        fileTreeManager.deleteFile(fileNameWithPath);
        return "success";
    }


    /**
     * one data node went offline
     */
    @EventListener
    public void listen(EurekaInstanceCanceledEvent event) throws IOException {
        synchronized (this) {
            String dataNodeUrl = "http://" + event.getServerId() + "/";
            System.err.println("Data node at \"" + dataNodeUrl + "\" went off line.");
            if (dataNodeManager.contain(dataNodeUrl)) {
                transferBlocksOnRemovedNode(dataNodeUrl);
                dataNodeManager.removeDataNode(dataNodeUrl);
                loadBalanceManager.removeDataNode(dataNodeUrl);
                fileBlockManager.removeDataNode(dataNodeUrl);
                System.err.println(event.getServerId() + "\t" + event.getAppName() + " removed");
            } else {
                System.err.println("Data node at " + dataNodeUrl +" was removed before.");
            }
            showManagers();
        }
    }

    /**
     * register a data node
     * @param event
     */
    @EventListener
    public void listen(EurekaInstanceRegisteredEvent event) {
        synchronized (this) {
            InstanceInfo instanceInfo = event.getInstanceInfo();
            String dataNodeUrl = instanceInfo.getHomePageUrl();
            System.err.println("Find a new data node at " + dataNodeUrl + ".");
            if (dataNodeManager.contain(dataNodeUrl)) {
                System.err.println("Data node at " + dataNodeUrl + " already exists.");
            } else {
                dataNodeManager.addDataNode(dataNodeUrl);
                loadBalanceManager.addDataNode(dataNodeUrl);
                transferBlocksToNewNode(dataNodeUrl);
                System.err.println("Data node at " + dataNodeUrl + " registered.");
            }
            showManagers();
        }
    }

    private boolean transferBlocksToNewNode(String newNodeURL) {
        try {
            Map<String, String> fileName_originalDataNoteURLs = loadBalanceManager.fileName_originalDataNoteURLs(newNodeURL);
            for (Map.Entry<String, String> transfer : fileName_originalDataNoteURLs.entrySet()) {
                String oldURL = transfer.getValue();
                String blockedFileName = transfer.getKey();
                File file = getFileBlock(oldURL, blockedFileName);
                sendFileBlock(newNodeURL, file);
//                if (!STAND_ALONE) {
                    askNodeToDeleteFileBlock(oldURL, blockedFileName);
//                }
                Pair<String, Long> fb = parseBlockedFileName(blockedFileName);
                dataNodeManager.transferBlock(fb.getKey(), fb.getValue(), oldURL, newNodeURL);
                fileBlockManager.transferBlock(fb.getKey(), fb.getValue(), oldURL, newNodeURL);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    private boolean transferBlocksOnRemovedNode(String URL) {
        try {
            Map<String, String> fileName_replaceDataNoteURLs = loadBalanceManager.fileName_replaceDataNoteURLs(URL);
            List<Pair<String, Long>> file_blocks = dataNodeManager.getFileBlocks(URL);
            for (Pair<String, Long> file_block : file_blocks) {
                // find dataNodes to get the removed file blocks
                String fileName = file_block.getKey();
                long blockIndex = file_block.getValue();
                List<String> URLs = fileBlockManager.getDataNodeURL(fileName, blockIndex);
                // don't need the removed data node
                // don't need the nodes that possess the file block
                URLs.removeIf(d -> d.equals(URL) || dataNodeManager.getFileBlocks(d).contains(fileName));

                if (URLs.size() == 0) {
                    System.err.println("Fail to find replica for block " + blockIndex + " of file " + fileName + ".");
                } else {
                    // download file block
                    String dataNodeURL = URLs.get(0);
                    String blockedFileName = blockedFileName(fileName, blockIndex);
                    File slicedFile = getFileBlock(dataNodeURL, blockedFileName);

                    // find a new node to put the file block
                    String replaceDataNodeURL = fileName_replaceDataNoteURLs.get(blockedFileName);
                    if (replaceDataNodeURL == null) {
                        System.err.println("Fail to find target data node to transfer block " + blockIndex + " of file " + fileName + ".");
                    } else {
                        // upload to the new node
                        boolean success = sendFileBlock(replaceDataNodeURL, slicedFile);
                        System.out.println("block " + blockIndex + " to data node at " + URL + " : " +
                                (success ? "success ." : "already exists ."));
                        if (success) {
                            dataNodeManager.addFileBlock(replaceDataNodeURL, fileName, blockIndex);
                            fileBlockManager.addFileBlock(replaceDataNodeURL, fileName, blockIndex);
                        }

                        slicedFile.delete();
                    }
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private void showManagers() {
        if (TEST_MODE) {
            dataNodeManager.show();
            fileBlockManager.show();
            loadBalanceManager.show();
            System.err.println();
        }
    }

    private void emptyTmpDir() {
        try {
            FileSystemUtils.deleteRecursively(Paths.get(blockFileDir).toFile());
            Files.createDirectories(Paths.get(blockFileDir));
            FileSystemUtils.deleteRecursively(Paths.get(downloadedFileDir).toFile());
            Files.createDirectories(Paths.get(downloadedFileDir));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * one pulse from a data node
     * @param event
     */
    @EventListener
    public void listen(EurekaInstanceRenewedEvent event) {
        System.err.println("Got a pulse from " + event.getServerId() + "\t" + event.getAppName() + ".");
//        nameNodeService.registerNewDataNode(dataNodeUrl);

    }

    /**
     * ready to register data nodes
     * @param event
     */
    @EventListener
    public void listen(EurekaRegistryAvailableEvent event) {
        System.err.println("Eureka registry is now available.");
    }

    /**
     * start eureka server
     * @param event
     */
    @EventListener
    public void listen(EurekaServerStartedEvent event) {
        System.err.println("Eureka Server started.");
        new File(blockFileDir).mkdirs();
        new File(downloadedFileDir).mkdirs();
        loadBalanceManager.setNumVisualNodes(VISUAL_NODE_NUM);
    }
}
