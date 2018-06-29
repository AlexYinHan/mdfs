package com.alex.sa.mdfs.datanode;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import javafx.beans.binding.ObjectExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import com.alex.sa.mdfs.datanode.storage.StorageFileNotFoundException;
import com.alex.sa.mdfs.datanode.storage.StorageService;

@RestController
public class FileSystemController {

    private final StorageService storageService;
    private Map<String, BlockInfo> fileName_fileInfo = new HashMap<>();

    @Autowired
    public FileSystemController(StorageService storageService) {
        this.storageService = storageService;
    }

    @GetMapping("/")
    public Map<String, BlockInfo> listUploadedFiles() throws IOException {
        return fileName_fileInfo;
    }

    @GetMapping("/files/{filename:.+}")
    @ResponseBody
    public ResponseEntity<Resource> serveFile(@PathVariable String filename) {

        Resource file = storageService.loadAsResource(filename);
        return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + file.getFilename() + "\"").body(file);
    }

    @DeleteMapping("/files/{filename:.+}")
    @ResponseBody
    public String deleteFile(@PathVariable String filename) {
        storageService.delete(filename);
        fileName_fileInfo.remove(filename);
        return "success";
    }

    @DeleteMapping("/allFiles")
    @ResponseBody
    public String deleteAll() {
        storageService.deleteAll();
        fileName_fileInfo.clear();
        return "success";
    }

    @PostMapping("/")
    public String handleFileUpload(@RequestParam("file") MultipartFile file) {
        String fileName = file.getOriginalFilename();
        // check if the file already exists
        if (fileName_fileInfo.keySet().contains(fileName)) {
            System.err.println("File already exists : " + fileName + " .");
            return "File already exists";
        }

        // upload to file system
        storageService.store(file);

        // record file/block information
        long fileSize = file.getSize();
        BlockInfo blockInfo = new BlockInfo(fileName, fileSize);
        fileName_fileInfo.put(fileName, blockInfo);

        return "success";
    }

    @ExceptionHandler(StorageFileNotFoundException.class)
    public ResponseEntity<?> handleStorageFileNotFound(StorageFileNotFoundException exc) {
        return ResponseEntity.notFound().build();
    }

}
