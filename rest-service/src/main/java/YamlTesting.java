import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.nio.file.Files;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookupFactory;
public class YamlTesting {
    public static void main(String[] args) {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            File f = new File("src/main/java/Test.yaml");
            System.out.println(StringLookupFactory.INSTANCE.dnsStringLookup().lookup("address|nuwans-MacBook-Pro.local"));
            StringSubstitutor env = new StringSubstitutor(StringLookupFactory.INSTANCE.environmentVariableStringLookup());
            String contents =
          env.replace(new String(Files.readAllBytes(f.toPath())));
            AppConfig config = mapper.readValue(contents, AppConfig.class);
            System.out.println(ReflectionToStringBuilder.toString(config,ToStringStyle.MULTI_LINE_STYLE));
            config.getList().stream().map(s->s.toObj()).forEach(System.out::println);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}