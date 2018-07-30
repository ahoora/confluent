package prozess.collaboration.generic;

import org.apache.avro.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;

@Configuration
public class SchemaConfiguration {

    @Bean
    public Schema entitySchema() throws IOException {
        return new Schema.Parser().parse(new ClassPathResource("entity.avsc").getInputStream());
        //return new Schema.Parser().parse(new File("entity.avsc"));
    }

    @Bean
    public Schema entityRequestSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        parser.parse(new ClassPathResource("entity.avsc").getInputStream());
        return parser.parse(new ClassPathResource("requested-entity.avsc").getInputStream());
    }

    @Bean
    public Schema entityValidationSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        parser.parse(new ClassPathResource("entity.avsc").getInputStream());
        parser.parse(new ClassPathResource("requested-entity.avsc").getInputStream());
        return parser.parse(new ClassPathResource("validated-entity.avsc").getInputStream());
    }
}
