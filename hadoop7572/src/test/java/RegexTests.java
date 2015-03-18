import mappers.namemappers.PairNameMapper;
import org.junit.Test;

/**
 * Created by alabdullahwi on 3/18/2015.
 */
public class RegexTests {


    private PairNameMapper mapper = new PairNameMapper() ;

    @Test
    public void isMovieIDRegexTests() {
        String movieID = "102132";
        org.junit.Assert.assertTrue(mapper.isMovieID(movieID) == true);
        org.junit.Assert.assertTrue(mapper.isMovieID(" 12312 213") == false);
    }



    @Test
    public void isPairRegexTests() {

        org.junit.Assert.assertTrue(mapper.isPair("<1, 2>") == true) ;
        org.junit.Assert.assertTrue(mapper.isPair("<1,2>") == false ) ;

    }


}
