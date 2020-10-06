package io.dogy.util;

import org.rocksdb.RocksDBException;

public interface IStatusMonitor {

    CurrentStatusInfo getCurrentStatusInfo() throws RocksDBException;

}
