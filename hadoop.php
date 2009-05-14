<?php

final class Hadoop
{
    private $_ipath;
    private $_opath;
    private $_path;
    private $_jar = "contrib/streaming/hadoop-0.18.3-streaming.jar";
    private $_tmp;
    private $_id;


    function __construct()
    {
        $this->_tmp = dirname(__FILE__)."/run/";
        $this->_id  = getmypid();
    }


    function Run()
    {
        $cmd = $this->_getCmd();
        die("$cmd\n");
        
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
        $map  = file_get_contents(dirname(__FILE__)."/map.php");

        /* extract the class code */
        $info = new ReflectionClass($job);
        $code = explode("\n", file_get_contents($info->getFileName()));
        $code = array_slice($code, $info->getStartLine()-1, $info->getEndLine()-2);
        $code = implode("\n", $code);

        /* save the map */
        $map = str_replace("hadoop.php", __FILE__, $map);
        $map = str_replace("/*name*/", $info->getName(), $map);
        $map = str_replace("/*class*/", $code, $map);
        file_put_contents($this->_getFileName("map"), $map);
        chmod($this->_getFileName("map"),0777);
        
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

        $cmd = sprintf("%sbin/hadoop jar %s -input %s -output %s -mapper %s -reducer %s ", $path, $path.$jarpath, $ipath, $opath, $this->_getFileName("map"), $this->_getFileName("reduce"));

        return $cmd;
    }
}

abstract class Job {
    final function EmitIntermediate($key, $value)
    {
        printf("%s\t%s%d",$key, $value, PHP_EOL);
    }

    final function RunMap()
    {
        while (($line = fgets(STDIN)) !== false) {
            list($key, $value) = explode("\t", $line);
            $this->map($key, $value);
        }

    }

    abstract protected function map($key, $value);
    abstract protected function reduce($iterable);
}

?>
