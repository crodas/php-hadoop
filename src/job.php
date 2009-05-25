<?php

/**
 *  Job
 *
 *  
 */
abstract class Job extends Hadoop
{
    /**
     *  
     *
     */
    final function EmitIntermediate($key, &$value)
    {
        $val = serialize($value);
        echo "$key\t$val\n";
    }

    final function Emit($key, &$value)
    {
        $val = serialize($value);
        echo "$key\t$val\n";
    }

    final function unserialize(&$data)
    {
        static $serfalse = false;

        if ($serfalse===false) {
            $serfalse = serialize(false);
        }

        if ($data===$serfalse || ($ret=@unserialize($data)) !== false) {
            $data = $ret;
        }
    }

    final function runMap()
    {
        $this->__init();
        while (($line = fgets(STDIN)) !== false) {
            $line = trim($line);
            if (strlen($line) == 0) {
                continue;
            }
            $input = $this->mapParser($line);

            if (trim($input[0]) == "") {
                throw new Exception($line);
            }

            $this->unserialize($input[1]);
            $this->map($input[0], $input[1]);
        }

    }

    protected function mapParser($line)
    {
        return explode("\t", $line, 2);
    }

    final function runReduce()
    {
        $this->__init();
        $values  = array();new parray();
        $lastkey = null;
 
        while (($line = fgets(STDIN)) !== false) {
            $key = $value = null;
            list($key, $value) = explode("\t", $line, 2);
            if ($key===null || $value===null) {
                continue;
            }

            if ($lastkey!=null && $lastkey != $key) {
                $this->reduce($lastkey, $values);
                $values = array();
            }
            
            $this->unserialize($value);
            $values[] = $value;
            $lastkey = $key;
        }
        $this->reduce($key, $values);

    }

    private function _execReduce(PArray $obj)
    {
        foreach ($obj->getKeys() as $id) {
            $val = & $obj->get($id);
            if (count($val) == 0) {
                continue;
            }
            $this->reduce($id, $val);
        }
        $obj = null;
        $obj = new parray;
    }


    function progress($date, $time, $type, $class, $text)
    {
        print "$date - $time = $text\n";
    }

    function __init() {}
    abstract protected function __config();
    abstract protected function map($key, &$value);
    abstract protected function reduce($key, &$iterable);
}

?>
