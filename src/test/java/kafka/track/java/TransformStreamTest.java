package kafka.track.java;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TransformStreamTest {

   @Test
   void should_convert_movie() {
      var transformStream = new TransformStream();
      Movie movie = transformStream.convertRawMovie(new RawMovie(294L, "Tree of Life::2011", "drama"));

      assertThat(movie.getId()).isEqualTo(294L);
      assertThat(movie.getTitle()).isEqualTo("Tree of Life");
      assertThat(movie.getGenre()).isEqualTo("drama");
   }
}
