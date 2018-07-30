package prozess.collaboration;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.springframework.boot.jackson.JsonComponent;
import prozess.model.ValidatedEntity;

import java.io.IOException;

@JsonComponent
public class ValidatedEntitySerializer extends JsonSerializer<ValidatedEntity> {

    @Override
    public void serialize(ValidatedEntity value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        AvroMapper mapper = new AvroMapper();
        mapper.writerFor(ValidatedEntity.class)
              .writeValue(jgen, value);
    }
}
