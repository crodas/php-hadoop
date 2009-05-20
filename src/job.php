<?php

abstract class Job
{
    final function EmitIntermediate($key, &$value)
    {
        if (!is_scalar($value)) {
            $value = "\t\t\t".serialize($value);
        }
        printf("%s\t%s\n",$key, $value);
    }

    final function Emit($key, &$value)
    {
        if (!is_scalar($value)) {
            $value = "\t\t\t".serialize($value);
        }
        printf("%s\t%s\n",$key, $value);
    }

    final function RunMap()
    {
        $this->__hadoop_init();
        while (($line = fgets(STDIN)) !== false) {
            $line = substr($line, 0, strlen($line)-1);
            if (strlen($line) == 0) {
                continue;
            }
            $input = $this->map_parser($line);
            if (count($input) == 1) {
                $input[1] = $input[0];
                $input[0] = null;
            }
            if (substr($input[1], 0,3) === "\t\t\t") {
                $input[1] = unserialize(substr($input[1],3));
            }
            $this->map($input[0], $input[1]);
        }

    }

    protected function map_parser($line)
    {
        return explode("\t", $line, 2);
    }

    final function RunReduce()
    {
        $this->__hadoop_init();
        $values = new parray();
 
        while (($line = fgets(STDIN)) !== false) {
            $key = $value = null;
            list($key, $value) = explode("\t", $line, 2);
            if ($key===null || $value===null) {
                continue;
            }
            
            if (substr($value, 0,3) === "\t\t\t") {
                $value = unserialize(substr($value,3));
            } else {
                $value = substr($value,0,strlen($value) -1);
            }
            $values->add($key, $value);
        }

        foreach ($values->getKeys() as $id) {
            $val = & $values->get($id);
            if (count($val) == 0) {
                continue;
            }
            $this->reduce($id, $val);
        }
        $values = null;
    }

    function __hadoop_init() {}

    abstract protected function map($key, &$value);
    abstract protected function reduce($key, &$iterable);
}

?>
