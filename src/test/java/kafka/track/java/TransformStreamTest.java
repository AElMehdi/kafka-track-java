package kafka.track.java;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TransformStreamTest {

   @Test
   void should_convert_movie() {
      var transformStream = new TransformStream();
      Movie movie = transformStream.convertRawMovie(new RawMovie(278L, "a title", "A genre"));

      Assertions.assertThat(movie).isNotNull();
   }
}
