<?php

Hadoop::import("mem-file.php");
Hadoop::import("job.php");


ini_set("memory_limit", "100M");


final class Hadoop
{
    private $_ipath;
    private $_opath;
    private static $_path;
    private $_jar = "contrib/streaming/hadoop-0.18.3-streaming.jar";
    private $_tmp;
    private $_id;
    private $_reduce = 1;


    function __construct()
    {
        $this->_tmp = dirname(__FILE__)."/run/";
        $this->_id  = getmypid();
    }

    static function import($file)
    {
        include(dirname(__FILE__)."/$file");
    }

    static function getFile($file)
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

    function setNumberOfReduces($number)
    {
        if ((int)$number < 1) {
            return false;
        }
        $this->_reduce = $number;
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
        $ini  = $info->getStartLine()-1;
        $end  = $info->getEndLine();
        $code = array_slice($code, $ini, $end - $ini);
        $code = implode("\n", $code);

        /* save the map */
        $map = str_replace("/*name*/", $info->getName(), $map);
        if (array_search($info->getFileName(), get_included_files()) === 0) {
            $map = str_replace("/*class*/", $code, $map);
        }
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
        $path    = self::$_path;

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


?>
