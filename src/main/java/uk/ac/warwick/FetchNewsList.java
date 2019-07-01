package uk.ac.warwick;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public class FetchNewsList {
    static final String APPLE_DAILY_URL = "https://tw.appledaily.com/search/ajaxresult";

    public static ArrayNode get(String date, int page){
        RestTemplate restTemplate = new RestTemplate();
        String url = getUrlBase(page, date).build().toString();
        return restTemplate.getForEntity(url, ArrayNode.class).getBody();
    }

    private static UriComponentsBuilder getUrlBase(int page, String date) {
        return UriComponentsBuilder.fromHttpUrl(APPLE_DAILY_URL)
                .queryParam("querystrS", "1=1")
                .queryParam("sort", "time")
                .queryParam("searchType", "all")
                .queryParam("dateStart", date)
                .queryParam("dateEnd", date)
                .queryParam("page", page);
    }
}
