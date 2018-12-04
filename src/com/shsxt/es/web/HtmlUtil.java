package com.shsxt.es.web;

import net.htmlparser.jericho.Source;

import java.io.File;
import java.io.IOException;

public class HtmlUtil {
    /**
     * 解析 html
     * @param file
     * @return
     * @throws IOException
     */
    public static HtmlBean parseHtml(File file) throws IOException {

        HtmlBean htmlBean = new HtmlBean();

        Source source = new Source(file);

        //etl: extract transfer  load

        String title = source.getFirstElement("title").getTextExtractor().toString();

        String content = source.getTextExtractor().toString();
        // file.getAbsolutePath()   G:\teach\doc\elasticsearch\data\www.shsxt.com\2017\recollections_1205\676.html
        int index = file.getAbsolutePath().indexOf("www.shsxt.com");

        String url = file.getAbsolutePath().substring(index);

        htmlBean.setContent(content);
        htmlBean.setTitle(title);
        htmlBean.setUrl(url);

        return  htmlBean;
    }
}
