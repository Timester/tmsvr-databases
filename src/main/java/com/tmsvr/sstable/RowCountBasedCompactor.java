package com.tmsvr.sstable;

import java.util.ArrayList;
import java.util.List;

public class RowCountBasedCompactor implements Compactor {

    private static final int COMPACTION_SIZE_LIMIT = 10;

    @Override
    public List<SSTable> compact(List<SSTable> tables) {

        List<SSTable> newTables = new ArrayList<>(tables.size());
        List<SSTable> tablesToMerge = new ArrayList<>(tables.size());

        for (SSTable table : tables) {
            if (table.getSize() > COMPACTION_SIZE_LIMIT) {
                newTables.add(table);
            } else {
                tablesToMerge.add(table);
            }
        }



        return newTables;
    }

    private SSTable merge(SSTable table1, SSTable table2) {
        return null;
    }
}
