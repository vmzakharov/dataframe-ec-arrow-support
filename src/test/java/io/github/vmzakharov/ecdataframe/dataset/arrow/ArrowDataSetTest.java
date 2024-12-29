package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArrowDataSetTest
{
    @Test
    public void basicReadDataFrameFromArrow()
    {
        ArrowDataSetSchema schema = new ArrowDataSetSchema()
                .addColumn("foo", STRING)
                .addColumn("bar", INT)
                .addColumn("baz", FLOAT);

        BufferAllocator allocator = new RootAllocator();

        ArrowDataSet dataSet = new ArrowDataSet("data set", schema);
        VectorSchemaRoot schemaRoot = dataSet.initializeVectorSchemaRoot(allocator);

        dataSet
                .setVectorValues("foo", "Alice", "Bob", "Carl", "Diane")
                .setVectorValues("bar", 10, 12, 11, 14)
                .setVectorValues("baz", 123.45f, 222.33f, 323.45f, 456.78f);

        schemaRoot.setRowCount(4);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("foo").addIntColumn("bar").addFloatColumn("baz")
                .addRow("Alice", 10, 123.45f)
                .addRow("Bob", 12, 222.33f)
                .addRow("Carl", 11, 323.45f)
                .addRow("Diane", 14, 456.78f);

        DataFrame fromArrow = dataSet.loadAsDataFrame();

        DataFrameTestUtil.assertEquals(expected, fromArrow);
    }

    @Test
    public void readDataFrameFromArrow()
    {
        ArrowDataSet dataSet = this.createPopulatedArrowDataSet();

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("aString").addIntColumn("anInt").addLongColumn("aLong")
                .addFloatColumn("aFloat").addDoubleColumn("aDouble")
                .addDateColumn("aDate").addDateTimeColumn("aDateTime")
                .addDecimalColumn("aDecimal").addBooleanColumn("aBoolean")
                .addRow(null, 10, 210L, 123.45f, 2123.45, LocalDate.of(2024, 9, 18), null, BigDecimal.valueOf(1234, 2), true)
                .addRow("Bob", null, 212L, 222.33f, 2222.33, null, LocalDateTime.of(2021, 4, 20, 6, 8, 4), null, false)
                .addRow("Carl", 11, null, 323.45f, null, LocalDate.of(2024, 10, 20), LocalDateTime.of(2022, 8, 10, 12, 28, 24), BigDecimal.valueOf(567, 1), null)
                .addRow("Diane", 14, 214L, null, 2456.78, LocalDate.of(2024, 12, 22), LocalDateTime.of(2023, 12, 1, 18, 38, 34), BigDecimal.valueOf(10, 3), false);

        DataFrame fromArrow = dataSet.loadAsDataFrame();

        DataFrameTestUtil.assertEquals(expected, fromArrow);
    }

    @Test
    public void readDataFrameFromArrowNoExplicitSchema()
    {
        try (VectorSchemaRoot source = this.createPopulatedArrowDataSet().vectorSchemaRoot())
        {
            ArrowDataSet dataSet = new ArrowDataSet("data set");

            DataFrame fromArrow = dataSet.loadAsDataFrame(source);

            DataFrame expected = new DataFrame("expected")
                    .addStringColumn("aString").addIntColumn("anInt").addLongColumn("aLong")
                    .addFloatColumn("aFloat").addDoubleColumn("aDouble")
                    .addDateColumn("aDate").addDateTimeColumn("aDateTime")
                    .addDecimalColumn("aDecimal").addBooleanColumn("aBoolean")
                    .addRow(null, 10, 210L, 123.45f, 2123.45, LocalDate.of(2024, 9, 18), null, BigDecimal.valueOf(1234, 2), true)
                    .addRow("Bob", null, 212L, 222.33f, 2222.33, null, LocalDateTime.of(2021, 4, 20, 6, 8, 4), null, false)
                    .addRow("Carl", 11, null, 323.45f, null, LocalDate.of(2024, 10, 20), LocalDateTime.of(2022, 8, 10, 12, 28, 24), BigDecimal.valueOf(567, 1), null)
                    .addRow("Diane", 14, 214L, null, 2456.78, LocalDate.of(2024, 12, 22), LocalDateTime.of(2023, 12, 1, 18, 38, 34), BigDecimal.valueOf(10, 3), false);

            DataFrameTestUtil.assertEquals(expected, fromArrow);
        }
    }

    @Test
    public void basicWriteDataFrameToArrow()
    {
        DataFrame dataFrame = new DataFrame("df")
                .addStringColumn("foo").addIntColumn("bar").addFloatColumn("baz")
                .addRow("Alice", 10, 123.45f)
                .addRow("Bob", 12, 222.33f)
                .addRow("Carl", 11, 323.45f)
                .addRow("Diane", 14, 456.78f);

        BufferAllocator allocator = new RootAllocator();

        ArrowDataSetSchema expectedSchema = new ArrowDataSetSchema()
                .addColumn("foo", STRING)
                .addColumn("bar", INT)
                .addColumn("baz", FLOAT);

        ArrowDataSet expectedDataSet = new ArrowDataSet("expected data set", expectedSchema);
        VectorSchemaRoot expectedSchemaRoot = expectedDataSet.initializeVectorSchemaRoot(allocator);

        expectedDataSet
                .setVectorValues("foo", "Alice", "Bob", "Carl", "Diane")
                .setVectorValues("bar", 10, 12, 11, 14)
                .setVectorValues("baz", 123.45f, 222.33f, 323.45f, 456.78f);

        expectedSchemaRoot.setRowCount(4);

        ArrowDataSet dataSet = new ArrowDataSet("data set");
        VectorSchemaRoot result = dataSet.toVectorSchemaRoot(dataFrame, allocator);

        assertEquals(expectedSchemaRoot.getRowCount(), result.getRowCount());
        assertTrue(expectedSchemaRoot.equals(result));
    }

    @Test
    public void writeDataFrameToArrow()
    {
        DataFrame dataFrame = new DataFrame("frame of data")
                .addStringColumn("aString").addIntColumn("anInt").addLongColumn("aLong")
                .addFloatColumn("aFloat").addDoubleColumn("aDouble")
                .addDateColumn("aDate").addDateTimeColumn("aDateTime")
                .addDecimalColumn("aDecimal").addBooleanColumn("aBoolean")
                .addRow(null, 10, 210L, 123.45f, 2123.45, LocalDate.of(2024, 9, 18), null, BigDecimal.valueOf(1234, 2), true)
                .addRow("Bob", null, 212L, 222.33f, 2222.33, null, LocalDateTime.of(2021, 4, 20, 6, 8, 4), null, false)
                .addRow("Carl", 11, null, 323.45f, null, LocalDate.of(2024, 10, 20), LocalDateTime.of(2022, 8, 10, 12, 28, 24), BigDecimal.valueOf(567, 1), null)
                .addRow("Diane", 14, 214L, null, 2456.78, LocalDate.of(2024, 12, 22), LocalDateTime.of(2023, 12, 1, 18, 38, 34), BigDecimal.valueOf(10, 3), false);

        ArrowDataSet dataSet = new ArrowDataSet("data set");
        try (
                BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot vectorSchemaRoot = dataSet.toVectorSchemaRoot(dataFrame, allocator);
                VectorSchemaRoot expected = this.createPopulatedArrowDataSet().vectorSchemaRoot();
        )
        {
            assertEquals(4, vectorSchemaRoot.getRowCount());
            assertTrue(expected.equals(vectorSchemaRoot));
            assertTrue(expected.approxEquals(vectorSchemaRoot));
        }
    }

    private ArrowDataSet createPopulatedArrowDataSet()
    {
        ArrowDataSetSchema schema = new ArrowDataSetSchema()
                .addColumn("aString", STRING)
                .addColumn("anInt", INT)
                .addColumn("aLong", LONG)
                .addColumn("aFloat", FLOAT)
                .addColumn("aDouble", DOUBLE)
                .addColumn("aDate", DATE)
                .addColumn("aDateTime", DATE_TIME)
                .addColumn("aDecimal", DECIMAL)
                .addColumn("aBoolean", BOOLEAN);

        ArrowDataSet dataSet = new ArrowDataSet("data set", schema);

        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot schemaRoot = dataSet.initializeVectorSchemaRoot(allocator);

        dataSet
                .setVectorValues("aString", null, "Bob", "Carl", "Diane")
                .setVectorValues("anInt", 10, null, 11, 14)
                .setVectorValues("aLong", 210L, 212L, null, 214L)
                .setVectorValues("aFloat", 123.45f, 222.33f, 323.45f, null)
                .setVectorValues("aDouble", 2123.45, 2222.33, null, 2456.78)
                .setVectorValues("aDate", LocalDate.of(2024, 9, 18), null, LocalDate.of(2024, 10, 20), LocalDate.of(2024, 12, 22))
                .setVectorValues("aDateTime", null, LocalDateTime.of(2021, 4, 20, 6, 8, 4),
                        LocalDateTime.of(2022, 8, 10, 12, 28, 24), LocalDateTime.of(2023, 12, 1, 18, 38, 34))
                .setVectorValues("aDecimal", BigDecimal.valueOf(1234, 2), null, BigDecimal.valueOf(567, 1), BigDecimal.valueOf(10, 3))
                .setVectorValues("aBoolean", true, false, null, false)
        ;

        schemaRoot.setRowCount(4);

        return dataSet;
    }
}
