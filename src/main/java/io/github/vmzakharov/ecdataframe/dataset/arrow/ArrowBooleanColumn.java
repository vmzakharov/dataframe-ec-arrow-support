package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfBooleanColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfBooleanColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;

public class ArrowBooleanColumn
extends ArrowColumn
{
    public ArrowBooleanColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(Types.MinorType.BIT.getType()), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.BOOLEAN;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (boolean) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfBooleanColumn booleanColumn = (DfBooleanColumn) dfColumn;

        this.setVectorValues(booleanColumn::isNull, booleanColumn::getBoolean, vector, dfColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToBooleanFunction valueGetter,
            FieldVector vector,
            int size)
    {
        BitVector booleanVector = (BitVector) vector;

        booleanVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                booleanVector.setNull(i);
            }
            else
            {
                booleanVector.setSafe(i, valueGetter.valueOf(i) ? 1 : 0);
            }
        }

        booleanVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        BitVector bitVector = (BitVector) vector;

        DfBooleanColumnStored dfColumn = (DfBooleanColumnStored) dataFrame.newColumn(this.name(), this.type());

        for (int i = 0; i < bitVector.getValueCount(); i++)
        {
            if (bitVector.isNull(i))
            {
                dfColumn.addEmptyValue();
            }
            else
            {
                dfColumn.addBoolean(bitVector.get(i) == 1, false);
            }
        }
    }
}
