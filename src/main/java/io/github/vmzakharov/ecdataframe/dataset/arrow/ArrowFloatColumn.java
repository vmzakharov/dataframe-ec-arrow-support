package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfFloatColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfFloatColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.IntToFloatFunction;

public class ArrowFloatColumn
extends ArrowColumn
{
    public ArrowFloatColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.FLOAT;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (float) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfFloatColumn floatColumn = (DfFloatColumn) dfColumn;

        this.setVectorValues(floatColumn::isNull, floatColumn::getFloat, vector, dfColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToFloatFunction valueGetter,
            FieldVector vector,
            int size)
    {
        Float4Vector floatVector = (Float4Vector) vector;

        floatVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                floatVector.setNull(i);
            }
            else
            {
                floatVector.setSafe(i, valueGetter.valueOf(i));
            }
        }

        floatVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        Float4Vector floatVector = (Float4Vector) vector;

        DfFloatColumnStored dfColumn = (DfFloatColumnStored) dataFrame.newColumn(this.name(), this.type());

        for (int i = 0; i < floatVector.getValueCount(); i++)
        {
            if (floatVector.isNull(i))
            {
                dfColumn.addEmptyValue();
            }
            else
            {
                dfColumn.addFloat(floatVector.get(i));
            }
        }
    }
}
