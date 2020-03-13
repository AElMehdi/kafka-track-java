package kafka.track.java;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class Movie implements SpecificRecord {

   private final long id;
   private final String title;
   private final int releaseYear;
   private final String genre;

   public Movie(long id, String title, int releaseYear, String genre) {
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

   public int getReleaseYear() {
      return releaseYear;
   }

   public String getGenre() {
      return genre;
   }

   @Override
   public void put(int i, Object v) {

   }

   @Override
   public Object get(int i) {
      return null;
   }

   @Override
   public Schema getSchema() {
      return null;
   }
}
