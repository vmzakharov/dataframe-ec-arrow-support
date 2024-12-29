package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfStringColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.IntToObjectFunction;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import java.nio.charset.StandardCharsets;

public class ArrowStringColumn
extends ArrowColumn
{
    public ArrowStringColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(new ArrowType.Utf8()), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.STRING;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (String) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfStringColumn stringColumn = (DfStringColumn) dfColumn;

        this.setVectorValues(stringColumn::isNull, stringColumn::getTypedObject, vector, stringColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToObjectFunction<String> valueGetter,
            FieldVector vector,
            int size)
    {
        VarCharVector varCharVector = (VarCharVector) vector;
        varCharVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                varCharVector.setNull(i);
            }
            else
            {
                varCharVector.setSafe(i, valueGetter.valueOf(i).getBytes(StandardCharsets.UTF_8));
            }
        }

        varCharVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        VarCharVector strVector = (VarCharVector) vector;

        MutableList<String> columnValues = Lists.mutable.of();

        for (int i = 0; i < strVector.getValueCount(); i++)
        {
            columnValues.add(strVector.isNull(i) ? null : new String(strVector.get(i)));
        }

        dataFrame.addStringColumn(this.name(), columnValues);
    }
}
