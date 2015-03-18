import mappers.FrequentItemsetMapper;
import org.junit.Test;

import java.util.List;

/**
 * Created by alabdullahwi on 3/18/2015.
 */
public class CombinatonItemSetTests {

    @Test
    public void itemsetCombinationTest() {
        Integer[] arr =  { 1,2,3,4,5 } ;
        List<String> out = new FrequentItemsetMapper().doCombine(arr);
        for (String el : out ) {
            System.out.println(el);
        }
    }


}
