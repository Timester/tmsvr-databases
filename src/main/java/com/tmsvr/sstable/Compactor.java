package com.tmsvr.sstable;

import java.util.List;

public interface Compactor {
    List<SSTable> compact(List<SSTable> tables);
}
