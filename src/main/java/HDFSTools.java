import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

public class HDFSTools {

    private static Configuration conf = new Configuration();
    private static FileSystem fs;
    private static int counter = 0;
    private static Schema schemaAvro;
    private static MessageType schemaParquet;

    static {
        System.setProperty("hadoop.home.dir", "/");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private final static String FILE_TEMP = "/temp.avro";
    private static String AVRO_PATH = "/user/datasets/expedia/";
    private static String PARQUET_PATH = "/user/datasets/weather/year=2016/month=10/day=1/part-00140-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet";

    public static Schema getSchemaAvro() {
        return schemaAvro;
    }

    public static int getCounter() {
        return counter;
    }

    public static MessageType getSchemaParquet() {
        return schemaParquet;
    }

    public static void setAvroPath(String avroPath) {
        AVRO_PATH = avroPath;
    }

    public static void setParquetPath(String parquetPath) {
        PARQUET_PATH = parquetPath;
    }

    public static void main(String[] args) throws IOException {
        /*
         * Iterate on whole avro(expedia) dataset
         * to get scheme and count rows
         */
        avroDatasetIterator();
        System.out.println("Avro Scheme:");
        System.out.println(schemaAvro.toString(true));
        System.out.println("Number of rows in expedia dataset:" + counter);
        /*
         * Get parquet scheme from one parquet file
         * and print it
         */
        schemaParquet = getParquetScheme();
        System.out.println(schemaParquet);
    }

    private static void avroDatasetIterator() throws IOException {
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
                new Path(AVRO_PATH), true);
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            avroFileTools(fileStatus.getPath());
        }
    }

    private static void avroFileTools(Path path) {
        /*
         * Reading avro files to the temporary file to use it with
         * DataFileReader and datum reader to get avro scheme
         */
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (InputStream input = fs.open(path);
             DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(FILE_TEMP), datumReader)) {

            File tempFile = new File(FILE_TEMP);
            if (tempFile.exists()) {
                tempFile.delete();
            }
            Files.copy(input, Paths.get(FILE_TEMP));
            schemaAvro = dataFileReader.getSchema();

           /*
            *Count rows in the expedia dataset
            */
            Iterator it = dataFileReader.iterator();
            if (!it.hasNext()) {
                throw new IOException("No content");
            }
            Object firstRec = it.next();
            if (firstRec instanceof GenericFixed || firstRec instanceof GenericArray) {
                throw new IOException("not a top level record");
            }
            if (schemaAvro.getType() != Schema.Type.RECORD) {
                throw new IOException("Top-level schema instance must be of type Schema.Type.RECORD");
            }

            GenericRecord cur;
            do {
                counter++;
                if (it.hasNext()) {
                    cur = (GenericRecord) it.next();
                } else {
                    cur = null;
                }
            } while (cur != null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static MessageType getParquetScheme() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(PARQUET_PATH), conf))) {
            return reader.getFooter().getFileMetaData().getSchema();
        } catch (IOException e) {
            System.out.println("Something went wrong with parquet file");
            e.printStackTrace();
        }
        throw new IOException();
    }
}
