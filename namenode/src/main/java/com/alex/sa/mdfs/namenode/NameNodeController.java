package com.alex.sa.mdfs.namenode;

import com.netflix.appinfo.InstanceInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.netflix.eureka.server.event.*;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

@RestController
public class NameNodeController {
    // block
    @Value(value="${block.default-size}")
    public int BLOCK_SIZE;
    @Value(value="${block.default-replicas}")
    public int REPLICA_NUM;

    private DataNodeManager dataNodeManager = new DataNodeManager();
    private FileBlockManager fileBlockManager = new FileBlockManager();
    private FileTreeManager fileTreeManager = new FileTreeManager();

    private static String blockFileDir = "tmp/fileBlocks/";
    private static String downloadedFildDir = "tmp/downloadedFiles/";


    @GetMapping("/allFiles")
    public Map<String, List<String>> listSystem() {
        return fileBlockManager.listAll();
    }

    @GetMapping("/{filename:.+}")
    @ResponseBody
    public ResponseEntity<Resource> serveFile(@PathVariable String filename) {
        try {
            // save downloaded file in a temp path
            File downloadFile = new File(downloadedFildDir + filename);
            downloadFile.createNewFile();
            // use FileChannel to get a joint file
            FileChannel downloadFileChannel = new FileOutputStream(downloadFile).getChannel();

            // get file blocks and append them to the downloaded file
            long numBlocks = fileTreeManager.getNumBlocks(filename);
            for (int blockIndex = 0; blockIndex < numBlocks; blockIndex ++) {
                List<String> dataNodeURLS = fileBlockManager.getDataNodeURL(filename, blockIndex);
                String blockedFileName = blockedFileName(filename, blockIndex);
                for (String dataNodeURL : dataNodeURLS) {
                    if (dataNodeManager.isValid(dataNodeURL)) {
                        String resourceURL = dataNodeURL + "files/" + blockedFileName;
                        UrlResource urlResource = new UrlResource(new URL(resourceURL));
                        InputStream inputStream = urlResource.getInputStream();
                        byte[] bytes = new byte[BLOCK_SIZE];
                        int blockSize = inputStream.read(bytes);
                        File slicedFile = writeBlockedFile(filename, bytes, blockIndex, blockSize);
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

    @PostMapping("/")
    public String handleFileUpload(@RequestParam("file") MultipartFile file) {
        try {
            String fileName = file.getOriginalFilename();
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
                    // prepare params
                    FileSystemResource resource = new FileSystemResource(slicedFile);
                    MultiValueMap<String, Object> parameters = new LinkedMultiValueMap<>();
                    parameters.add("file", resource);

                    // send post request
                    RestTemplate rest = new RestTemplate();
                    String response = rest.postForObject(URL, parameters, String.class);
                    System.out.println("block " + blockIndex + " to data node at " + URL + " : " + response);
                    dataNodeManager.addFileBlock(URL, fileName, blockIndex);
                    fileBlockManager.addFileBlock(URL, fileName, blockIndex);
                }

                slicedFile.delete();
            }
            fileTreeManager.addFile(fileName, numBlocks);
            return "success";
        } catch (IOException e) {
            e.printStackTrace();
            return "fail";
        }
    }

    private List<String> getTargetDataNodes(File file) {
        // TODO: load balance and based on numRepeats
        return dataNodeManager.getAllDataNodeURL();
    }

    private String blockedFileName(String fileName, long blockIndex) {
        return "block_" + blockIndex + "_" + fileName;
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
    /**
     * one data node went offline
     */
    @EventListener
    public void listen(EurekaInstanceCanceledEvent event) throws IOException {
        String dataNodeUrl = "http://" + event.getServerId() + "/";
        System.err.println("Data node at \"" + dataNodeUrl + "\" went off line.");
        dataNodeManager.removeDataNode(dataNodeUrl);
        System.err.println(event.getServerId() + "\t" + event.getAppName() + " removed");
    }

    /**
     * register a data node
     * @param event
     */
    @EventListener
    public void listen(EurekaInstanceRegisteredEvent event) {
        InstanceInfo instanceInfo = event.getInstanceInfo();
        String dataNodeUrl = instanceInfo.getHomePageUrl();
        System.err.println("Find a new data node at " + dataNodeUrl + ".");
        dataNodeManager.addDataNode(dataNodeUrl);
        System.err.println("Data node at " + dataNodeUrl + " registered.");
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
        new File(downloadedFildDir).mkdirs();

    }
}