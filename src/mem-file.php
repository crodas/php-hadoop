<?php

final class PArray {
    private $_dir;
    private $_data = array();
    private $_cnt;
    private $_count = 0;
    private $_disk  = 0;
    private $_db;
    private $_limit = 70;
    private $_threshold = 40;

    function __construct()
    {
        $file = tempnam("/tmp", "");
        unlink($file);
        $this->_dir = "{$file}.db";
        $this->_db  = dba_open($this->_dir, "c","db4") or die("Unable to run reducer, please ensure you have dba + db4");
    }

    private function _swap()
    {
        $count = & $this->_count;
        $disk  = & $this->_disk;

        /* freeing memory and dump it to disk  {{{ */
        $limit = $this->_limit;
        if ((memory_get_usage()/(1024*1024)) <= $limit) {
            return; /* nothing to free-up, so return */
        }
        $limit -= $this->_threshold; /* dump X megabytes to disk */
        $wdata  = & $this->_data;
        end($wdata);
        fwrite(STDERR, "Freeing ".ceil(memory_get_usage()/(1024*1024))."M ".time()."\n");
        $i = $count;
        while ((memory_get_usage()/(1024*1024)) >= $limit)  {
            if (--$i < 0) {
                break;
            }
            $xdata  = current($wdata);
            if (!is_array($xdata)) {
                prev($wdata);
                continue;
            }
            $xkey   = key($wdata);
            fwrite(STDERR, "\t\tAttempt to free $xkey\n");
            $serial = serialize( $xdata );
            if (!dba_insert($xkey, $serial, $this->_db)) {
                dba_replace($xkey, $serial, $this->_db);
            }
            unset($wdata[$xkey]);
            unset($xdata);
            unset($serial);
            $wdata[ $xkey ] = true;
            prev($wdata);
            $disk++;
        }
        dba_sync($this->_db);
        fwrite(STDERR, "\tFreed ".ceil(memory_get_usage()/(1024*1024))."M ".time()."\n");
        /* }}} */
    }

    function add($key, &$value) 
    {
        $data  = & $this->_data[$key];
        $count = & $this->_count;

        if (!isset($data)) {
            $data = array();
            $count++;
        }

        if ($data === true) {
            $data = unserialize(dba_fetch($key, $this->_db));
        }
        $data[] = $value;
        $this->_swap();
    }

    function & get($key)
    {
        if ($this->_data[$key] === true) {
            $this->_swap();
            $this->_disk--;
            $value = unserialize(dba_fetch($key, $this->_db));
            return $value;
        }
        return $this->_data[$key];
    }

    function getKeys()
    {
        fwrite(STDERR, "Getting values\n");
        return array_keys($this->_data);
    }

    function __destruct()
    {
        dba_close($this->_db);
        unlink($this->_dir);
    }
}


?>
