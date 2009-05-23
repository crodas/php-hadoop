<?php


abstract class Job extends Hadoop
{
    const SERIALIZED="/serial/";

    final function EmitIntermediate($key, &$value)
    {
        $val = self::SERIALIZED.serialize($value);
        echo "$key\t$val\n";
    }

    final function Emit($key, &$value)
    {
        $val = self::SERIALIZED.serialize($value);
        echo "$key\t$val\n";
    }

    final function runMap()
    {
        $len  = strlen(self::SERIALIZED);
        $this->__init();
        while (($line = fgets(STDIN)) !== false) {
            $line = substr($line, 0, strlen($line)-1);
            if (strlen($line) == 0) {
                continue;
            }
            $input = $this->mapParser($line);
            if (count($input) == 1) {
                $input[1] = $input[0];
                $input[0] = null;
            }
            if (strpos($input[1],self::SERIALIZED) === 0) {
                $input[1] = unserialize(substr($input[1], $len));
            }
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
        $len     = strlen(self::SERIALIZED);
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
            
            if (strpos($value,self::SERIALIZED) === 0) {
                $value = unserialize(substr($value, $len));
            }
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
