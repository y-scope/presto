package com.yscope.presto.split;

import java.util.List;
import com.yscope.presto.ClpTableLayoutHandle;
import com.yscope.presto.ClpSplit;

public interface ClpSplitProvider {
    List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle);
}
