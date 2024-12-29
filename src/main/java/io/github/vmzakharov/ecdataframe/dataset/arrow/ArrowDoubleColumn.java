package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.IntToDoubleFunction;

public class ArrowDoubleColumn
extends ArrowColumn
{
    public ArrowDoubleColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.DOUBLE;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (double) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfDoubleColumn floatColumn = (DfDoubleColumn) dfColumn;

        this.setVectorValues(floatColumn::isNull, floatColumn::getDouble, vector, dfColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToDoubleFunction valueGetter,
            FieldVector vector,
            int size)
    {
        Float8Vector doubleVector = (Float8Vector) vector;

        doubleVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                doubleVector.setNull(i);
            }
            else
            {
                doubleVector.setSafe(i, valueGetter.valueOf(i));
            }
        }

        doubleVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        Float8Vector doubleVector = (Float8Vector) vector;

        DfDoubleColumnStored dfColumn = (DfDoubleColumnStored) dataFrame.newColumn(this.name(), this.type());

        for (int i = 0; i < doubleVector.getValueCount(); i++)
        {
            if (doubleVector.isNull(i))
            {
                dfColumn.addEmptyValue();
            }
            else
            {
                dfColumn.addDouble(doubleVector.get(i));
            }
        }
    }
}
