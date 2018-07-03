package cn.edu.nju.sa2017.pipefilter;

import org.springframework.batch.item.ItemProcessor;

public class CustomItemProcessor implements ItemProcessor<Report, Report> {

    public Report process(Report item) throws Exception {

        System.out.println("Processing..." + item);
        return item;
    }

}