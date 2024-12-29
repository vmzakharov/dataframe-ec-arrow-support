package io.github.vmzakharov.ecdataframe.dataset.arrow;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.eclipse.collections.api.block.function.primitive.IntToBooleanFunction;
import org.eclipse.collections.api.block.function.primitive.IntToObjectFunction;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import java.time.LocalDate;

public class ArrowDateColumn
extends ArrowColumn
{
    public ArrowDateColumn(String name)
    {
        super(name, new Field(name, FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null));
    }

    @Override
    public ValueType type()
    {
        return ValueType.DATE;
    }

    @Override
    public void setVectorValues(FieldVector vector, Object... values)
    {
        this.setVectorValues(index -> values[index] == null, index -> (LocalDate) values[index], vector, values.length);
    }

    @Override
    public void setVectorValuesFromDfColumn(FieldVector vector, DfColumn dfColumn)
    {
        DfDateColumn stringColumn = (DfDateColumn) dfColumn;

        this.setVectorValues(stringColumn::isNull, stringColumn::getTypedObject, vector, stringColumn.getSize());
    }

    private void setVectorValues(
            IntToBooleanFunction nullChecker,
            IntToObjectFunction<LocalDate> valueGetter,
            FieldVector vector,
            int size)
    {
        DateDayVector dateVector = (DateDayVector) vector;
        dateVector.allocateNew(size);

        for (int i = 0; i < size; i++)
        {
            if (nullChecker.valueOf(i))
            {
                dateVector.setNull(i);
            }
            else
            {
                dateVector.setSafe(i, (int) valueGetter.valueOf(i).toEpochDay());
            }
        }

        dateVector.setValueCount(size);
    }

    @Override
    public void addToDataFrameFromVector(DataFrame dataFrame, FieldVector vector)
    {
        DateDayVector dateVector = (DateDayVector) vector;

        MutableList<LocalDate> columnValues = Lists.mutable.of();

        for (int i = 0; i < dateVector.getValueCount(); i++)
        {
            columnValues.add(
                    dateVector.isNull(i) ? null : LocalDate.ofEpochDay(dateVector.get(i))
            );
        }

        dataFrame.addDateColumn(this.name(), columnValues);
    }
}
