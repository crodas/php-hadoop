<?php

ini_set("memory_limit", "100M");

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
        return;
        foreach (glob($this->_dir."/*") as $file) {
            unlink($file);
        }
        rmdir($this->_dir);
    }
}

final class Hadoop
{
    private $_ipath;
    private $_opath;
    private $_path;
    private $_jar = "contrib/streaming/hadoop-0.18.3-streaming.jar";
    private $_tmp;
    private $_id;
    private $_reduce = 4;


    function __construct()
    {
        $this->_tmp = dirname(__FILE__)."/run/";
        $this->_id  = getmypid();
    }


    function Run()
    {
        $cmd = $this->_getCmd();
        $p = popen("$cmd 2>&1", "r");
        while ($r = fread($p,1024)) {
            print $r;
        }
        fclose($p);
        @unlink($this->_getFileName("map"));
        @unlink($this->_getFileName("reduce"));
    }

    function setHome($file)
    {
        if (!is_dir($file)) {
            return False;
        }
        if (!is_file($file."/bin/hadoop")) {
            return False;
        }

        if ($file[strlen($file)-1] != "/") {
            $file .= "/";
        }
        $this->_path = $file;
        return True;
    }

    function setInput($path)
    {
        $this->_ipath = $path;
    }

    function setOutput($path)
    {
        $this->_opath = $path;
    }

    function setJob(Job $job)
    {
        $map    = file_get_contents(dirname(__FILE__)."/map.php");
        $reduce = file_get_contents(dirname(__FILE__)."/reduce.php");

        /* extract the class code */
        $info = new ReflectionClass($job);
        $code = explode("\n", file_get_contents($info->getFileName()));
        $code = array_slice($code, $info->getStartLine()-1, $info->getEndLine()-2);
        $code = implode("\n", $code);

        /* save the map */
        $map = str_replace("/*name*/", $info->getName(), $map);
        $map = str_replace("/*class*/", $code, $map);
        file_put_contents($this->_getFileName("map"), $map);
        chmod($this->_getFileName("map"),0777);

        /* save the reduce */
        $reduce = str_replace("/*name*/", $info->getName(), $reduce);
        $reduce = str_replace("/*class*/", $code, $reduce);
        file_put_contents($this->_getFileName("reduce"), $reduce);
        chmod($this->_getFileName("reduce"),0777);
    }

    private function _getFileName($name)
    {
        return $this->_tmp."/$name-".$this->_id.".php";
    }

    private function _getCmd()
    {
        $jarpath = $this->_jar;
        $ipath   = $this->_ipath;
        $opath   = $this->_opath;
        $path    = $this->_path;

        $cmd = sprintf("%sbin/hadoop jar %s -input %s -output %s -mapper %s -reducer %s  -jobconf mapred.reduce.tasks=%d -file %s -file %s -file %s", 
                $path, $path.$jarpath, $ipath, $opath, 
                basename($this->_getFileName("map")), 
                basename($this->_getFileName("reduce")), $this->_reduce,
                $this->_getFileName("map"), $this->_getFileName("reduce"),__FILE__
            );
        /* */
        $includes = get_included_files();
        for ($i=2; $i < count($includes); $i++) {
            $cmd .= " -file ".$includes[$i];
        }
        echo "$cmd\n\n\n";
        return $cmd;
    }
}

abstract class Job
{
    final function EmitIntermediate($key, $value)
    {
        printf("%s\t%s\n",$key, $value);
    }

    final function Emit($key, $value)
    {
        printf("%s\t%s\n",$key, $value);
    }

    final function RunMap()
    {
        while (($line = fgets(STDIN)) !== false) {
            $line = substr($line, 0, strlen($line)-1);
            if (strlen($line) == 0) {
                continue;
            }
            $this->map($line);
        }

    }

    final function RunReduce()
    {
        $values = new parray();
 
        while (($line = fgets(STDIN)) !== false) {
            $key = $value = null;
            list($key, $value) = explode("\t", $line, 2);
            if ($key===null || $value===null) {
                continue;
            }
            $values->$key = substr($value,0,strlen($value) -1);
        }

        foreach ($values->getKeys() as $id) {
            if (count($values->$id) == 0) {
                continue;
            }
            $this->reduce($id, $values->$id);
        }
        $values = null;
    }

    abstract protected function map($value);
    abstract protected function reduce($key, $iterable);
}

?>
