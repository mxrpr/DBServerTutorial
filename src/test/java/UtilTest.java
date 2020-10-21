import com.mixer.util.Leveinshtein;
import org.junit.Assert;
import org.junit.Test;

public class UtilTest {
    @Test
    public void testLeveinstein_0_distance() {
        int result = Leveinshtein.leveinshteinDistance("John", "John");
        Assert.assertEquals(0, result);
    }

    @Test
    public void testLeveinstein_1_distance() {
        int result = Leveinshtein.leveinshteinDistance("Cohn", "John");
        Assert.assertEquals(1, result);
    }
    @Test
    public void testLeveinstein_empty_strings() {
        int result = Leveinshtein.leveinshteinDistance("", "");
        Assert.assertEquals(0, result);
    }

    @Test
    public void testLeveinstein_null_string() {
        int result = Leveinshtein.leveinshteinDistance("", null);
        Assert.assertEquals(-1, result);
    }
}
