package arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class ArrowflightServer {


    public static void main1(String[] args) {
        Field age = new Field("age",
                FieldType.nullable(new ArrowType.Int(32, true)),
                /*children*/ null);
        Field name = new Field("name",
                FieldType.nullable(new ArrowType.Utf8()),
                /*children*/ null);
        Schema schema = new Schema(asList(age, name));
        try(
                BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                IntVector ageVector = (IntVector) root.getVector("age");
                VarCharVector nameVector = (VarCharVector) root.getVector("name");
        ){
            ageVector.allocateNew(3);
            ageVector.set(0, 10);
            ageVector.set(1, 20);
            ageVector.set(2, 30);
            nameVector.allocateNew(3);
            nameVector.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
            nameVector.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
            nameVector.set(2, "Mary".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(3);
            File file = new File("random_access_file.arrow");
            try (
                    FileOutputStream fileOutputStream = new FileOutputStream(file);
                    ArrowFileWriter writer = new ArrowFileWriter(root, /*provider*/ null, fileOutputStream.getChannel());
            ) {
                writer.start();
                writer.writeBatch();
                writer.end();
                System.out.println("Record batches written: " + writer.getRecordBlocks().size()
                        + ". Number of rows written: " + root.getRowCount());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try(
                BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                FileInputStream fileInputStream = new FileInputStream(new File("random_access_file.arrow"));
                ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);
        ){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                System.out.println("VectorSchemaRoot read: \n" + root.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
