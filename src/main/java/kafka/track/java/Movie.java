package kafka.track.java;

public class Movie {

   private final long id;
   private final String title;
   private final String releaseYear;
   private final String genre;

   public Movie(long id, String title, String releaseYear, String genre) {
      this.id = id;
      this.title = title;
      this.releaseYear = releaseYear;
      this.genre = genre;
   }

   public long getId() {
      return id;
   }

   public String getTitle() {
      return title;
   }

   public String getReleaseYear() {
      return releaseYear;
   }

   public String getGenre() {
      return genre;
   }
}
