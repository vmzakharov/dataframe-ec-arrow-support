package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.IntToIntFunction;

public class ArrowIntColumn
extends ArrowColumn
{
    public ArrowIntColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.INT;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (int) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfIntColumn intColumn = (DfIntColumn) dfColumn;

        this.setVectorValues(intColumn::isNull, intColumn::getInt, vector, dfColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToIntFunction valueGetter,
            FieldVector vector,
            int size)
    {
        IntVector intVector = (IntVector) vector;

        intVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                intVector.setNull(i);
            }
            else
            {
                intVector.setSafe(i, valueGetter.valueOf(i));
            }
        }

        intVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        IntVector intVector = (IntVector) vector;

        DfIntColumnStored dfColumn = (DfIntColumnStored) dataFrame.newColumn(this.name(), this.type());

        for (int i = 0; i < intVector.getValueCount(); i++)
        {
            if (intVector.isNull(i))
            {
                dfColumn.addEmptyValue();
            }
            else
            {
                dfColumn.addInt(intVector.get(i), false);
            }
        }
    }
}
