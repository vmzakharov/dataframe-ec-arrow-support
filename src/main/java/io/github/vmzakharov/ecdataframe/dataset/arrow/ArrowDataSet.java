package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataset.DataSetAbstract;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

public class ArrowDataSet
extends DataSetAbstract
{
    private ArrowDataSetSchema dataSetSchema;
    private VectorSchemaRoot vectorSchemaRoot;

    public ArrowDataSet(String newName, ArrowDataSetSchema arrowDataSetSchema)
    {
        super(newName);
        this.dataSetSchema = arrowDataSetSchema;
    }

    public ArrowDataSet(String newName)
    {
        super(newName);
    }

    public VectorSchemaRoot initializeVectorSchemaRoot(BufferAllocator allocator)
    {
        this.vectorSchemaRoot = VectorSchemaRoot.create(this.dataSetSchema.createArrowSchema(), allocator);
        this.vectorSchemaRoot.allocateNew();
        return this.vectorSchemaRoot;
    }

    /**
     * converts the tabular data in the instance of {@code VectorSchemaRoot} this data set represents to a data frame
     *
     * @return a data frame representation of the tabular data in the {@code VectorSchemaRoot}
     */
    public DataFrame loadAsDataFrame()
    {
        return this.loadAsDataFrame(this.vectorSchemaRoot);
    }

    /**
     * converts the tabular data in the instance of {@code VectorSchemaRoot} passed as the parameter to a data frame
     *
     * @param schemaRoot an instance of {@code VectorSchemaRoot} to be converted to a data frame
     * @return a data frame representation of the tabular data in {@code schemaRoot}
     */
    public DataFrame loadAsDataFrame(VectorSchemaRoot schemaRoot)
    {
        if (this.dataSetSchema == null)
        {
            this.dataSetSchema = this.createDataSetSchemaFrom(schemaRoot);
        }

        DataFrame dataFrame = new DataFrame("From Arrow");

        this.dataSetSchema.columns().each(
                col -> col.addToDataFrameFromVector(dataFrame, schemaRoot.getVector(col.name())
        ));

        dataFrame.seal();

        return dataFrame;
    }

    private ArrowDataSetSchema createDataSetSchemaFrom(VectorSchemaRoot schemaRoot)
    {
        List<Field> fields = schemaRoot.getSchema().getFields();

        ArrowDataSetSchema derivedSchema = new ArrowDataSetSchema();

        for (int i = 0; i < fields.size(); i++)
        {
            ArrowType fieldType = fields.get(i).getType();
            ValueType columnType;

            if (fieldType instanceof ArrowType.Int intType)
            {
                columnType = intType.getBitWidth() == 32 ? ValueType.INT : ValueType.LONG;
            }
            else if (fieldType instanceof ArrowType.FloatingPoint fpType)
            {
                columnType = fpType.getPrecision() == FloatingPointPrecision.DOUBLE ? ValueType.DOUBLE : ValueType.FLOAT;
            }
            else if (fieldType instanceof ArrowType.Decimal)
            {
                columnType = ValueType.DECIMAL;
            }
            else if (fieldType instanceof ArrowType.Utf8)
            {
                columnType = ValueType.STRING;
            }
            else if (fieldType instanceof ArrowType.Date dateType)
            {
                columnType = dateType.getUnit() == DateUnit.DAY ? ValueType.DATE : ValueType.DATE_TIME;
            }
            else if (fieldType instanceof ArrowType.Time)
            {
                columnType = ValueType.DATE_TIME;
            }
            else if (fieldType instanceof ArrowType.Bool)
            {
                columnType = ValueType.BOOLEAN;
            }
            else
            {
                throw new RuntimeException("Unsupported Arrow field type: " + fieldType);
            }

            derivedSchema.addColumn(fields.get(i).getName(), columnType);
        }

        return derivedSchema;
    }

    @Override
    public void openFileForReading()
    {
//        int rowCount = this.vectorSchemaRoot.getRowCount();
    }

    @Override
    public Object next()
    {
        return null;
    }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public void close()
    {
        this.vectorSchemaRoot.close();
    }

    public ArrowDataSet setVectorValues(String columnName, Object... values)
    {
        this.dataSetSchema.column(columnName).setVectorValues(this.vectorSchemaRoot.getVector(columnName), values);
        return this;
    }

    public VectorSchemaRoot vectorSchemaRoot()
    {
        return this.vectorSchemaRoot;
    }

    public VectorSchemaRoot toVectorSchemaRoot(DataFrame dataFrame, BufferAllocator allocator)
    {
        if (this.dataSetSchema == null)
        {
            this.dataSetSchema = new ArrowDataSetSchema();
            dataFrame.getColumns().each(col -> this.dataSetSchema.addColumn(col.getName(), col.getType()));
        }

        this.initializeVectorSchemaRoot(allocator);

        this.dataSetSchema.columns().each(arrowColumn -> arrowColumn.setVectorValuesFromDfColumn(
                    this.vectorSchemaRoot.getVector(arrowColumn.name()),
                    dataFrame.getColumnNamed(arrowColumn.name())
                )
        );

        this.vectorSchemaRoot.setRowCount(dataFrame.rowCount());
        return this.vectorSchemaRoot;
    }
}
