package kafka.track.java;

public class TransformStream {

   public Movie convertRawMovie(RawMovie rawMovie) {
      String[] splitTitle = rawMovie.getTitle().split("::");

      String title = splitTitle[0];
      String releaseYear = splitTitle[1];

      return new Movie(rawMovie.getId(), title, releaseYear, rawMovie.getGenre());
   }
}
