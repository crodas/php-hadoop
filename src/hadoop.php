<?php
Hadoop::import("mem-file.php");
Hadoop::import("job.php");


ini_set("memory_limit", "200M");


define("HAS_NO_REDUCER",    2);

/**
 *  Hadoop Class.
 *
 *
 *
 */
abstract class Hadoop
{
    private static $_path = false;
    private static $_injob = false;
    private static $_fncWatch = false;
    private $_ipath = false;
    private $_opath = false;
    private $_jar = "contrib/streaming/hadoop-0.18.3-streaming.jar";
    private $_tmp;
    private $_reduce;
    private $_map = false;
    private $_nomap = false;
    private $_noreduce = false;

    final function __construct()
    {
        if (self::$_injob) {
            return;
        }
        $this->__config();
        if ($this->_ipath === false || $this->_opath === false) {
            throw new Exception("No input or output path configured");
        }
        if (!self::$_path) {
            throw new Exception("No Hadoop-home");
        }
        $this->_tmp = array(tempnam("/tmp", "map"), tempnam("/tmp", "red"));
        $this->_execTask();
    }

    final protected function setOption($options)
    {
        if ($options & HAS_NO_REDUCER) {
            $this->_noreduce = true;
        }
    }

    final static function import($file)
    {
        include(dirname(__FILE__)."/$file");
    }

    final static function initMapper()
    {
        self::$_injob = true;
    }

    final static function initReducer()
    {
        self::$_injob = true;
    }

    final static function getFile($file)
    {
        $home = self::$_path;
        $tmp = tmpfile();
        $p = popen("${home}/bin/hadoop fs -cat ${file}", "r");
        while ($r = fread($p,1024)) {
            fwrite($tmp, $r);
        }
        fclose($p);
        rewind($tmp);
        return $tmp;
    }

    final function setReducers($number)
    {
        if ((int)$number < 1) {
            return false;
        }
        $this->_reduce = $number;
    }

    final function setMappers($number)
    {
        if ((int)$number < 1) {
            $this->_map = false;
            return false;
        }
        $this->_map = $number;
    }


    private function _execTask()
    {
        $this->_buildTempFiles();
        $cmd = $this->_getCmd();
        $p = popen("$cmd 2>&1", "r");
        while ($r = fgets($p,1024)) {
            $r = trim($r);
            $parts = explode(" ", $r, 5);
            if (count($parts) != 5) {
                continue;
            }
            list($date, $time, $type, $class, $text) = $parts;
            if ($type == "ERROR") {
                fclose($p);
                @unlink($this->_getFileName("map"));
                @unlink($this->_getFileName("reduce"));
                throw new Exception($text);
            }
            $this->progress($date, $time, $type, $class, $text);
        }
        fclose($p);
        @unlink($this->_getFileName("map"));
        @unlink($this->_getFileName("reduce"));
    }

    public static function setHome($file)
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
        self::$_path = $file;
        return True;
    }

    final protected function setInput($path)
    {
        $this->_ipath = $path;
    }

    final protected function setOutput($path)
    {
        $this->_opath = $path;
    }

    private function _buildTempFiles()
    {
        $map    = file_get_contents(dirname(__FILE__)."/map.php");
        $reduce = file_get_contents(dirname(__FILE__)."/reduce.php");
        $includ = '';
        $files  = get_included_files();

        for ($i=1; $i < count($files); $i++) {
            $includ .= "require_once('".basename($files[$i])."');\n";
        }

        /* extract the class code */
        $info = new ReflectionClass($this);
        $code = explode("\n", file_get_contents($info->getFileName()));
        $ini  = $info->getStartLine()-1;
        $end  = $info->getEndLine();
        $code = array_slice($code, $ini, $end - $ini);
        $code = implode("\n", $code);

        /* save the map */
        $map = str_replace("/*name*/", $info->getName(), $map);
        $map = str_replace("/*include*/", $includ, $map);
        $map = str_replace("/*hadoop-home*/", self::$_path, $map);
        if (array_search($info->getFileName(), get_included_files()) === 0) {
            $map = str_replace("/*class*/", $code, $map);
        }
        file_put_contents($this->_getFileName("map"), $map);
        chmod($this->_getFileName("map"),0777);

        /* save the reduce */
        $reduce = str_replace("/*name*/", $info->getName(), $reduce);
        $reduce = str_replace("/*include*/", $includ, $reduce);
        $reduce = str_replace("/*hadoop-home*/", self::$_path, $reduce);
        if (array_search($info->getFileName(), get_included_files()) === 0) {
            $reduce = str_replace("/*class*/", $code, $reduce);
        }
        file_put_contents($this->_getFileName("reduce"), $reduce);
        chmod($this->_getFileName("reduce"),0777);
    }

    private function _getFileName($name)
    {
        return $this->_tmp[ $name=="map" ? 0 : 1];
    }

    private function _getCmd()
    {
        $jarpath = $this->_jar;
        $ipath   = $this->_ipath;
        $opath   = $this->_opath;
        $path    = self::$_path;

        $cmd = sprintf("%sbin/hadoop jar %s -input %s -output %s -file %s -file %s -file %s", 
                $path, $path.$jarpath, $ipath, $opath, 
                $this->_getFileName("map"), $this->_getFileName("reduce"),__FILE__
            );

        $cmd .= " -mapper ".basename($this->_getFileName("map"));

        if (!$this->_noreduce) {
            $cmd .= " -reducer ".basename($this->_getFileName("reducer"));
        }

        /* set number of mappers */
        if (is_int($this->_map)) {
            $cmd .= " -jobconf mapred.map.tasks=".$this->_map;
        }

        /* set number of reducers */
        if (is_int($this->_reduce)) {
            $cmd .= " -jobconf mapred.reduce.tasks=".$this->_reduce;
        }

        /* */
        $includes = get_included_files();
        for ($i=2; $i < count($includes); $i++) {
            $cmd .= " -file ".$includes[$i];
        }
        return $cmd;
    }

    abstract function progress($date, $time, $type, $class, $text);
}


?>
