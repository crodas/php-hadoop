<?php

final class PArray {
    private $_dir;
    private $_data = array();
    private $_cnt;
    private $_count = 0;
    private $_disk  = 0;
    private $_db;

    function __construct()
    {
        $this->_dir = tempnam("/tmp", "").".db";
        $this->_db  = dba_open($this->_dir, "c","db4");
    }

    function __set($key, $value) 
    {
        $data  = & $this->_data[$key];
        $count = & $this->_count;
        $disk  = & $this->_disk;

        if (!isset($data)) {
            $data = array();
            $count++;
        }

        if ($data === true) {
            $data = unserialize(dba_fetch($key, $this->_db));
            $disk--;
        }
        $data[] = $value;

        /* freeing memory and dump it to disk  {{{ */
        $limit = 70; /* 38 megabytes */
        if ((memory_get_usage()/(1024*1024)) < $limit ) {
            return; /* nothing to free-up, so return */
        }
        $limit -= 35; /* dump 5 megabytes to disk */
        $wdata  = & $this->_data;
        end($wdata);
        fwrite(STDERR, "Freeing ".ceil(memory_get_usage()/(1024*1024))."M ".time()."\n");
        $i = $count;
        while ((memory_get_usage()/(1024*1024)) > $limit  && current($wdata)) {
            if (--$i == 0) {
                break;
            }
            $xkey  = key($wdata);
            $xdata = current($wdata);
            if (!is_array($xdata)) {
                prev($wdata);
                continue;
            }
            $serialize = serialize( $xdata );
            if (!dba_insert($xkey, $serialize, $this->_db)) {
                dba_replace($xkey, $serialize, $this->_db);
            }
            unset($wdata[$xkey]);
            unset($xdata);
            $wdata[ $xkey ] = true;
            prev($wdata);
            $disk++;
        }
        unset($xdata);
        fwrite(STDERR, "\tFreed ".ceil(memory_get_usage()/(1024*1024))."M ".time()."\n");
        /* }}} */
    }

    function __get($key)
    {
        if ($this->_data[$key] === true) {
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
