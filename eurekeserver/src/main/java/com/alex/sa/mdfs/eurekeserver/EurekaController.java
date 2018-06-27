package com.alex.sa.mdfs.eurekeserver;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EurekaController {
    @GetMapping("/")
    public String listWholeFileSystem() {
        return "hello";
    }
}
