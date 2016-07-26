package net.deelam.graphtools.importer;

public interface ImporterFactory {
  Importer create(SourceData sd);
}
