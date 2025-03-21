package org.apache.hop.pipeline.jdbc;

import lombok.Getter;
import lombok.Setter;

public abstract class Schemas {

  public static Field of(String name, int type, int size, int length) {
    return new Field(name, type, size, length);
  }

  public static Index of(String name, boolean unique, Field[] fields, double fd) {
    return new Index(name, unique, fields, fd);
  }

  public static TableSnap<?> of(Field[] fields, Index[] indexes) {
    return new TableSnap<>(fields, indexes);
  }

  @Getter
  public static class Field implements Comparable<Field> {
    private final String name;
    private final int type;
    private final int size;
    private final int length;

    Field(String name, int type, int size, int length) {
      this.name = name;
      this.type = type;
      this.size = size;
      this.length = length;
    }

    @Override
    public int compareTo(Field o) {
      return name.compareToIgnoreCase(o.getName());
    }
  }

  @Getter
  public static class Index implements Comparable<Index> {
    private final String name;
    private final boolean unique;
    private final Field[] fields;
    private final double fd;

    Index(String name, boolean unique, Field[] fields, double fd) {
      this.name = name;
      this.unique = unique;
      this.fields = fields;
      this.fd = fd;
    }

    @Override
    public int compareTo(Index o) {
      return name.compareToIgnoreCase(o.getName());
    }
  }

  @Getter
  @Setter
  public static class TableSnap<T> {
    private final Field[] fields;
    private final Index[] indexes;
    private String[] uselessIndexes;
    private String[] uselessFields;
    private long avgRowSize;
    private Comparable<T> latest;

    TableSnap(Field[] fields, Index[] indexes) {
      this.fields = fields;
      this.indexes = indexes;
    }
  }
}
