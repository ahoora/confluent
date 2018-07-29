package prozess.collaboration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;

@Configuration
public class SchemaConfiguration {

    @Bean
    public Schema entitySchema() throws IOException {
        return new Schema.Parser().parse(new ClassPathResource("schemas/entity.avsc").getInputStream());
        //return new Schema.Parser().parse(new File("entity.avsc"));
    }

    @Bean
    public Schema entityRequestSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        parser.parse(new ClassPathResource("schemas/entity.avsc").getInputStream());
        return parser.parse(new ClassPathResource("schemas/requested-entity.avsc").getInputStream());
    }

    @Bean
    public Schema entityValidationSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        parser.parse(new ClassPathResource("schemas/entity.avsc").getInputStream());
        parser.parse(new ClassPathResource("schemas/requested-entity.avsc").getInputStream());
        return parser.parse(new ClassPathResource("schemas/validated-entity.avsc").getInputStream());
    }
}
