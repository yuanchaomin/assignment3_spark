import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Chaomin on 6/12/2017.
 */
public class Set_completment_test {

    public static void main(String[] args) {
        String itemset_string = "1-2-3";
        String sub_itemset_string = "1-";
        String elementsA[] = itemset_string.split("-");
        String elementsB[] = sub_itemset_string.split("-");
        Set<String> set_A = new HashSet<String>(Arrays.asList(elementsA));
        Set<String> set_B = new HashSet<String>(Arrays.asList(elementsB));

        set_A.removeAll(set_B);
        String result = set_A
                .toString()
                .replaceAll("\\s+|\\[|\\]","")
                .replaceAll(",","-");

        String result2 = result + "-";
        System.out.println(result2);
    }
}
