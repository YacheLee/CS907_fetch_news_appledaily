package uk.ac.warwick;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

public class FetchNews {
    public static Document getDoc(String url) throws IOException {
        return Jsoup.connect(url).get();
    }

    public static String getHtml(Document document) throws IOException {
        return document.body().html();
    }

    public static String getText(String html){
        Document doc = Jsoup.parse(html);
        Elements select = doc.select(".ndArticle_contentBox .ndArticle_margin");
        Element iframe = select.select("iframe").first();
        if(iframe!=null){
            Elements iframeNextAll = iframe.nextElementSiblings();
            iframeNextAll.remove();
            iframe.remove();
        }

        select = doc.select(".ndArticle_contentBox .ndArticle_margin p");
        return select.text();
    }
}
