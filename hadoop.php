<?php

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
        $values = array();

        while (($line = fgets(STDIN)) !== false) {
            list($key, $value) = explode("\t", $line, 2);
            if (!isset($values[$key])) {
                $values[$key] = array();
            }
            $values[$key][] = substr($value,0,strlen($value) -1);
        }

        foreach (array_keys($values) as $id) {
            if (count($values[$id]) == 0) {
                continue;
            }
            $this->reduce($id, $values[$id]);
        }
    }

    abstract protected function map($value);
    abstract protected function reduce($key, &$iterable);
}

?>
