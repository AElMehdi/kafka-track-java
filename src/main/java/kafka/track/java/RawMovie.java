package kafka.track.java;

import java.util.StringJoiner;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class RawMovie implements SpecificRecord {
   private final long id;
   private final String title;
   private String genre;

   private RawMovie(Builder builder) {
      this.id = builder.id;
      this.title = builder.title;
      this.genre = builder.genre;
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

   public static class Builder {
      private long id;
      private String title;
      private String genre;

      public Builder id(long id) {
         this.id = id;
         return this;
      }

      public Builder title(String title) {
         this.title = title;
         return this;
      }

      public Builder genre(String genre) {
         this.genre = genre;
         return this;
      }

      public RawMovie build() {
         return new RawMovie(this);
      }

   }

   @Override
   public String toString() {
      return new StringJoiner(", ", RawMovie.class.getSimpleName() + "[", "]")
            .add("id=" + id)
            .add("title='" + title + "'")
            .add("genre='" + genre + "'")
            .toString();
   }
}
