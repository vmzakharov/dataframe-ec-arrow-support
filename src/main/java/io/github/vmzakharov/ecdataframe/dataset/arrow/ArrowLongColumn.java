package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.IntToLongFunction;

public class ArrowLongColumn
extends ArrowColumn
{
    public ArrowLongColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.LONG;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (long) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfLongColumn longColumn = (DfLongColumn) dfColumn;

        this.setVectorValues(longColumn::isNull, longColumn::getLong, vector, dfColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToLongFunction valueGetter,
            FieldVector vector,
            int size)
    {
        BigIntVector longVector = (BigIntVector) vector;

        longVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                longVector.setNull(i);
            }
            else
            {
                longVector.setSafe(i, valueGetter.valueOf(i));
            }
        }

        longVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        BigIntVector longVector = (BigIntVector) vector;

        DfLongColumnStored dfColumn = (DfLongColumnStored) dataFrame.newColumn(this.name(), this.type());

        for (int i = 0; i < longVector.getValueCount(); i++)
        {
            if (longVector.isNull(i))
            {
                dfColumn.addEmptyValue();
            }
            else
            {
                dfColumn.addLong(longVector.get(i), false);
            }
        }
    }
}
