package kafka.track.java;

public class RawMovie {
   private final long id;
   private final String title;
   private String genre;

   public RawMovie(long id, String title, String genre) {
      this.id = id;
      this.title = title;
      this.genre = genre;
   }

   public long getId() {
      return this.id;
   }

   public String getTitle() {
      return title;
   }

   public String getGenre() {
      return genre;
   }
}
